#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import re
from typing import Dict, List, Optional

_CREATE_TABLE_RE = re.compile(r"CREATE\s+TABLE\s+`?([\w_]+)`?\s*\(", re.IGNORECASE)


def _extract_columns(block: str) -> Dict[str, str]:
    cols: Dict[str, str] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        up = line.upper()
        if up.startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "KEY", "CONSTRAINT")):
            continue
        m = re.match(r"`?([A-Za-z0-9_]+)`?\s+([A-Za-z]+)(?:\s*\([^)]*\))?", line)
        if not m:
            continue
        col, typ = m.group(1), m.group(2).upper()
        cols[col] = typ
    return cols


def parse_schema(schema_sql_path: str) -> Dict[str, Dict[str, str]]:
    sql = Path(schema_sql_path).read_text(encoding="utf-8")
    tables: Dict[str, Dict[str, str]] = {}
    pos = 0
    while True:
        m = _CREATE_TABLE_RE.search(sql, pos)
        if not m:
            break
        tname = m.group(1)
        start = m.end()
        end = sql.find(");", start)
        if end == -1:
            end = sql.find(")\n", start)
            if end == -1:
                break
        block = sql[start:end]
        tables[tname] = _extract_columns(block)
        pos = end + 2
    return tables


def _find_csv_for_table(base: Path, t: str) -> Optional[Path]:
    candidates = [
        base / f"{t}.csv",
        base / f"{t}.CSV",
        base / f"{t.lower()}.csv",
        base / f"{t.upper()}.csv",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def _count_csv_rows(csv_path: Path) -> int:
    # Count lines and subtract header line if present (assumes header exists).
    cnt = 0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        for _ in f:
            cnt += 1
    return max(cnt - 1, 0)


def collect_row_counts(schema_dir: str, tables: Dict[str, Dict[str, str]]) -> Dict[str, int]:
    base = Path(schema_dir)
    counts: Dict[str, int] = {}
    for t in tables.keys():
        csv_path = _find_csv_for_table(base, t)
        counts[t] = _count_csv_rows(csv_path) if csv_path else 0
    return counts


def render_row_counts(counts: Dict[str, int], all_tables: List[str]) -> str:
    enriched = {t: counts.get(t, 0) for t in all_tables}
    ordered = sorted(enriched.items(), key=lambda x: (-x[1], x[0].lower()))
    lines: List[str] = []
    for t, n in ordered:
        lines.append(f"\"{t}\": {n} rows;")
    return "\n".join(lines)


def build_part2(schema_dir: str) -> str:
    schema_sql = Path(schema_dir) / "schema.sql"
    if not schema_sql.is_file():
        raise SystemExit(f"schema.sql not found in: {schema_dir}")
    tables = parse_schema(str(schema_sql))
    counts = collect_row_counts(schema_dir, tables)
    block = render_row_counts(counts, list(tables.keys()))
    return (
        "其中各表的行数从多到少分别为：\n\n"
        f"{block}\n\n"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="PART2: 统计各表 CSV 行数并渲染提示片段")
    ap.add_argument("schema_dir", help="目录，包含 schema.sql 和与表同名的 *.csv")
    ap.add_argument("--out", help="输出文件；省略则打印到标准输出")
    args = ap.parse_args()

    content = build_part2(args.schema_dir)
    if args.out:
        Path(args.out).write_text(content, encoding="utf-8")
    else:
        print(content)


if __name__ == "__main__":
    main()
