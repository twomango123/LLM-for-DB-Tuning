#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Usage: import_csvs.sh -d <database> [-h host] [-P port] [-u user] [-p password] [-D dir]

Imports CSVs (with header) in the directory into MySQL tables and applies schema.sql.

Options:
  -d    Database name (required)
  -h    MySQL host (default: 127.0.0.1)
  -P    MySQL port (default: 3306)
  -u    MySQL user (default: root)
  -p    MySQL password (default: prompt or use MYSQL_PWD env)
  -D    Directory containing schema.sql and *.csv (default: script directory)

Examples:
  ./import_csvs.sh -d tpcch
  ./import_csvs.sh -d csu_1 -u root -p secret -D /path/to/dir
USAGE
}

HOST="127.0.0.1"
PORT="3306"
USER="root"
PASS=""
DB=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DIR="$SCRIPT_DIR"

while getopts ":h:P:u:p:d:D:" opt; do
  case "$opt" in
    h) HOST="$OPTARG" ;;
    P) PORT="$OPTARG" ;;
    u) USER="$OPTARG" ;;
    p) PASS="$OPTARG" ;;
    d) DB="$OPTARG" ;;
    D) DIR="$OPTARG" ;;
    *) usage; exit 1 ;;
  esac
done

if [[ -z "$DB" ]]; then
  echo "[ERROR] Missing -d <database>" >&2
  usage
  exit 1
fi

if [[ ! -d "$DIR" ]]; then
  echo "[ERROR] Directory not found: $DIR" >&2
  exit 1
fi

SCHEMA_SQL="$DIR/schema.sql"
if [[ ! -f "$SCHEMA_SQL" ]]; then
  echo "[ERROR] schema.sql not found in $DIR" >&2
  exit 1
fi

MYSQL_BASE=( mysql --local-infile=1 -h "$HOST" -P "$PORT" -u "$USER" )
if [[ -n "$PASS" ]]; then
  export MYSQL_PWD="$PASS"
fi

echo "[INFO] Ensuring database exists: $DB"
"${MYSQL_BASE[@]}" -e "CREATE DATABASE IF NOT EXISTS \`$DB\` DEFAULT CHARACTER SET utf8mb4;" >/dev/null

echo "[INFO] Applying schema: $SCHEMA_SQL"
"${MYSQL_BASE[@]}" "$DB" < "$SCHEMA_SQL"

TMP_SQL="$(mktemp -t load_csvs.XXXXXX.sql)"
trap 'rm -f "$TMP_SQL"' EXIT

{
  echo "SET autocommit=0; SET FOREIGN_KEY_CHECKS=0; SET UNIQUE_CHECKS=0; SET NAMES utf8mb4;"

  shopt -s nullglob
  for f in "$DIR"/*.csv; do
    tbl="$(basename "$f" .csv)"
    # Build absolute path and SQL-escape single quotes in path
    absf="$(readlink -f "$f" || realpath "$f" 2>/dev/null || echo "$f")"
    path_esc=$(printf %s "$absf" | sed "s/'/''/g")

    # Extract column list from CSV header using Python's CSV parser
    cols=$(python3 - "$f" <<'PY'
import csv, sys
from pathlib import Path
fp = Path(sys.argv[1])
with fp.open(newline='') as g:
    r = csv.reader(g)
    header = next(r)
def bt(s: str) -> str:
    s = s.strip()
    s = s.replace('`','``')
    return f"`{s}`"
print(", ".join(bt(h) for h in header if h.strip()))
PY
    )

    echo "[INFO] Scheduling load: $tbl <- $(basename "$f")"
    printf "LOAD DATA LOCAL INFILE '%s' INTO TABLE \`%s\` CHARACTER SET utf8mb4\n" "$path_esc" "$tbl"
    printf "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED BY '"'\n"
    printf "LINES TERMINATED BY '\n' IGNORE 1 LINES (%s);\n\n" "$cols"
  done
  echo "COMMIT; SET FOREIGN_KEY_CHECKS=1; SET UNIQUE_CHECKS=1;"
} > "$TMP_SQL"

echo "[INFO] Loading CSVs from $DIR"
"${MYSQL_BASE[@]}" "$DB" < "$TMP_SQL"

echo "[INFO] Done."
