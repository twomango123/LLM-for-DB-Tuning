#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate meta.json for storage estimation using a MySQL database.

Inputs:
  - schema.sql path: used to determine which tables to include (and to scope FK discovery).
  - MySQL connection: used to fetch row counts, PK/FK, column types and perform sampling for varlen columns.

Computation rules (MySQL):
  - Fixed-length columns: estimated bytes = fixed_bytes_per_value * row_count.
  - Variable-length columns: sample with RAND() < ratio and use OCTET_LENGTH(col) to compute
    sample_total_bytes / sample_ratio as an estimate of total bytes; also derive avg bytes per value.
  - Null fraction: per table, sample with RAND() < ratio and compute SUM(col IS NULL) / sample_count.

Only table data is considered; index and storage engine overheads are ignored.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Reuse existing driver and helpers
try:
    from DataBase.MySQLDriver import MySQLDriver  # type: ignore
except Exception as e:  # pragma: no cover
    MySQLDriver = None  # type: ignore

try:
    from storage.estimator import ColumnStorageEstimator, is_varlen_type, fixed_size_bytes  # type: ignore
except Exception:
    ColumnStorageEstimator = None  # type: ignore
    def is_varlen_type(dialect: str, data_type: str) -> bool:  # fallback
        dt = data_type.lower()
        return any(x in dt for x in ["char", "text", "json", "blob", "varchar", "varbinary"])  # type: ignore
    def fixed_size_bytes(dialect: str, data_type: str) -> Optional[int]:  # type: ignore
        MYSQL_FIXED_SIZES = {
            "tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4, "integer": 4, "bigint": 8,
            "float": 4, "double": 8, "date": 3, "datetime": 5, "timestamp": 4, "time": 3,
            "year": 1, "bool": 1, "boolean": 1,
        }
        return MYSQL_FIXED_SIZES.get(data_type.lower())


def my_ident(name: str) -> str:
    return '`' + str(name).replace('`', '``') + '`'


# -----------------------------
# Minimal SQL parser for schema.sql
# -----------------------------
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?[\w]+`?\.)?`?([\w_]+)`?)\s*\(",
    re.IGNORECASE,
)


def parse_tables_from_schema(schema_sql_path: Path) -> List[str]:
    sql = schema_sql_path.read_text(encoding="utf-8", errors="ignore")
    tables: List[str] = []
    pos = 0
    seen: Set[str] = set()
    while True:
        m = _CREATE_TABLE_RE.search(sql, pos)
        if not m:
            break
        tname = m.group(1)
        if tname and tname not in seen:
            seen.add(tname)
            tables.append(tname)
        pos = m.end()
    return tables


# -----------------------------
# DB metadata helpers
# -----------------------------
def list_columns(drv: Any, table: str) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT COLUMN_NAME AS column_name,
               DATA_TYPE AS data_type,
               COLUMN_TYPE AS column_type,
               CHARACTER_MAXIMUM_LENGTH AS char_max_len,
               NUMERIC_PRECISION AS numeric_precision,
               NUMERIC_SCALE AS numeric_scale
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = {json.dumps(table)}
        ORDER BY ORDINAL_POSITION
    """
    return drv.execute_query(sql)


def get_primary_key(drv: Any, table: str) -> List[str]:
    sql = f"""
        SELECT COLUMN_NAME AS column_name
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = {json.dumps(table)}
          AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
    """
    rows = drv.execute_query(sql)
    return [r["column_name"] for r in rows]


def get_foreign_keys(drv: Any, tables: Set[str]) -> List[Dict[str, Any]]:
    """Return FK list in meta.json format, grouped by constraint with ordering.
    Only includes FKs whose both sides are in provided tables set.
    """
    sql = """
        SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME,
               REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME,
               ORDINAL_POSITION
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND REFERENCED_TABLE_NAME IS NOT NULL
        ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
    """
    rows = drv.execute_query(sql)
    groups: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ft = r["TABLE_NAME"]
        tt = r["REFERENCED_TABLE_NAME"]
        if ft not in tables or tt not in tables:
            continue
        key = r["CONSTRAINT_NAME"]
        g = groups.setdefault(key, {
            "from_table": ft, "from_columns": [],
            "to_table": tt, "to_columns": []
        })
        g["from_columns"].append(r["COLUMN_NAME"])
        g["to_columns"].append(r["REFERENCED_COLUMN_NAME"])
    return list(groups.values())


def estimate_row_count(drv: Any, table: str) -> Optional[int]:
    # Try EXPLAIN FORMAT=JSON first
    try:
        rows = drv.execute_query(f"EXPLAIN FORMAT=JSON SELECT * FROM {my_ident(table)}")
        raw = rows[0].get("EXPLAIN") if isinstance(rows[0], dict) else rows[0][0]
        qb = json.loads(raw).get("query_block", {})
        tbl = qb.get("table") or {}
        rp = tbl.get("rows_produced_per_join")
        if rp is not None:
            return int(rp)
        base = tbl.get("rows")
        if base is not None:
            return int(base)
    except Exception:
        pass
    # Fallback: information_schema.TABLES
    try:
        rows = drv.execute_query(
            f"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = {json.dumps(table)}"
        )
        if rows and rows[0].get("TABLE_ROWS") is not None:
            return int(rows[0]["TABLE_ROWS"])  # approx
    except Exception:
        pass
    # Last resort: COUNT(*)
    try:
        rows = drv.execute_query(f"SELECT COUNT(*) AS c FROM {my_ident(table)}")
        return int(rows[0]["c"]) if rows else None
    except Exception:
        return None


def sample_column_bytes(drv: Any, table: str, column: str, sample_ratio: float) -> Dict[str, Any]:
    tq = my_ident(table)
    cq = my_ident(column)
    sql = f"""
        WITH sample_stats AS (
            SELECT OCTET_LENGTH({cq}) AS single_byte, {sample_ratio} AS sample_ratio
            FROM {tq}
            WHERE RAND() < {sample_ratio}
        ),
        sample_total AS (
            SELECT SUM(single_byte) AS sample_total_bytes,
                   COUNT(*) AS sample_row_count,
                   AVG(single_byte) AS avg_single_byte,
                   sample_ratio
            FROM sample_stats
            GROUP BY sample_ratio
        )
        SELECT sample_row_count, sample_total_bytes, avg_single_byte, sample_ratio
        FROM sample_total
    """
    rows = drv.execute_query(sql)
    return rows[0] if rows else {
        "sample_row_count": 0, "sample_total_bytes": 0, "avg_single_byte": None, "sample_ratio": sample_ratio
    }


def sample_null_fracs(drv: Any, table: str, columns: List[str], sample_ratio: float) -> Dict[str, float]:
    if not columns:
        return {}
    tq = my_ident(table)
    proj = ["COUNT(*) AS sample_count"]
    for c in columns:
        cq = my_ident(c)
        proj.append(f"SUM({cq} IS NULL) AS null__{c}")
    select_list = ",\n               ".join(proj)
    sql = f"""
        SELECT {select_list}
        FROM {tq}
        WHERE RAND() < {sample_ratio}
    """
    rows = drv.execute_query(sql)
    if not rows:
        return {c: 0.0 for c in columns}
    r = rows[0]
    total = max(1, int(r.get("sample_count") or 0))
    out: Dict[str, float] = {}
    for c in columns:
        nulls = int(r.get(f"null__{c}") or 0)
        out[c] = float(nulls) / float(total) if total > 0 else 0.0
    return out


def _decimal_bytes_from_precision(prec: Optional[int]) -> Optional[int]:
    if prec is None:
        return None
    try:
        p = int(prec)
    except Exception:
        return None
    groups = p // 9
    rem = p % 9
    rem_bytes_map = {0: 0, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 4, 8: 4}
    return groups * 4 + rem_bytes_map.get(rem, 0)


def _fixed_size_mysql(dt: str,
                      column_type: Optional[str],
                      char_max_len: Optional[int],
                      num_prec: Optional[int],
                      num_scale: Optional[int]) -> Optional[int]:
    dtl = (dt or "").lower()
    # Prefer existing simple map first
    base = fixed_size_bytes("mysql", dtl)
    if base is not None:
        return base
    # CHAR/BINARY are fixed-width per definition (ignoring charset bytes)
    if dtl in ("char", "binary"):
        try:
            if char_max_len is not None:
                return int(char_max_len)
        except Exception:
            pass
    # DECIMAL/NUMERIC approximate bytes by precision groups
    if dtl in ("decimal", "numeric"):
        return _decimal_bytes_from_precision(num_prec)
    return None


def build_meta_for_tables(drv: Any,
                          tables: List[str],
                          sample_ratio: float,
                          min_sample_rows: int) -> Dict[str, Any]:
    meta: Dict[str, Any] = {"total_storage_bytes": 0, "tables": {}, "foreign_keys": [], "stats": {"predicates": {}, "joins": {}}}

    table_set = set(tables)

    for t in tables:
        row_count = estimate_row_count(drv, t) or 0
        # Effective ratio to ensure enough samples
        eff_ratio = sample_ratio
        if min_sample_rows and row_count:
            needed = float(min_sample_rows) / float(row_count)
            if needed > eff_ratio:
                eff_ratio = min(1.0, needed)

        pk_cols = get_primary_key(drv, t)
        col_rows = list_columns(drv, t)
        col_names = [r["column_name"] for r in col_rows]
        # One pass to get null fracs for all columns
        null_fracs = sample_null_fracs(drv, t, col_names, eff_ratio) if col_names else {}

        t_entry = {
            "row_count": int(row_count),
            "primary_key": pk_cols,
            "columns": {},
        }

        for r in col_rows:
            c = r["column_name"]
            dt = r["data_type"]
            ct = r.get("column_type")
            char_len = r.get("char_max_len")
            num_prec = r.get("numeric_precision")
            num_scale = r.get("numeric_scale")
            # Varlen types in MySQL: varchar/varbinary/text/blob/json
            dtl = (dt or "").lower()
            is_var = dtl in ("varchar", "varbinary", "text", "tinytext", "mediumtext", "longtext",
                             "blob", "tinyblob", "mediumblob", "longblob", "json")
            null_frac = float(null_fracs.get(c, 0.0))
            if is_var:
                s = sample_column_bytes(drv, t, c, eff_ratio)
                avg_b = s.get("avg_single_byte")
                avg_len = float(avg_b) if avg_b is not None else 0.0
            else:
                size = _fixed_size_mysql(dtl, ct, char_len, num_prec, num_scale)
                avg_len = float(size) if size is not None else 0.0
            t_entry["columns"][c] = {"avg_length": avg_len, "null_frac": null_frac}

        meta["tables"][t] = t_entry

    # Foreign keys (only among selected tables)
    try:
        meta["foreign_keys"] = get_foreign_keys(drv, table_set)
    except Exception:
        meta["foreign_keys"] = []

    # Total storage bytes
    total = 0.0
    for tname, t in meta["tables"].items():
        rc = int(t.get("row_count", 0))
        per_row = 0.0
        for cname, c in t.get("columns", {}).items():
            avg = float(c.get("avg_length", 0))
            null_frac = float(c.get("null_frac", 0))
            per_row += avg * (1.0 - null_frac)
        total += per_row * rc
    meta["total_storage_bytes"] = int(total)
    return meta


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate storage meta.json from MySQL + schema.sql")
    p.add_argument("--schema-sql", required=True, help="Path to initial schema.sql (used to scope tables/FKs)")
    p.add_argument("--host", required=True)
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--sample-ratio", type=float, default=0.01, help="Base sampling ratio for varlen/null estimation")
    p.add_argument("--min-sample-rows", type=int, default=100, help="Ensure at least this many sampled rows per table")
    p.add_argument("--out", required=True, help="Output meta.json path")
    return p


def connect_driver(args) -> Any:
    if MySQLDriver is None:
        raise RuntimeError("MySQL driver not available; ensure dependencies installed and repo layout intact.")
    cfg: Dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
    }
    drv = MySQLDriver(cfg)
    if not drv.connect():
        raise RuntimeError("Failed to connect to MySQL")
    return drv


def main(argv=None) -> None:
    ap = build_parser()
    args = ap.parse_args(argv)

    schema_path = Path(args.schema_sql)
    if not schema_path.exists():
        raise SystemExit(f"schema.sql not found: {schema_path}")
    sel_tables = parse_tables_from_schema(schema_path)
    if not sel_tables:
        raise SystemExit("No tables parsed from schema.sql; please check format")

    drv = connect_driver(args)
    try:
        meta = build_meta_for_tables(
            drv,
            tables=sel_tables,
            sample_ratio=args.sample_ratio,
            min_sample_rows=args.min_sample_rows,
        )
    finally:
        try:
            drv.disconnect()
        except Exception:
            pass

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(json.dumps({"ok": True, "output": str(out_path), "tables": len(meta.get("tables", {}))}, ensure_ascii=False))


if __name__ == "__main__":
    main()
