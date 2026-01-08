#!/usr/bin/env python3

# python3 scripts/build_prompt.py spider_data/spider_data/database_mysql/csu_1 --prompt prompt.md
import argparse
import os
import re
from typing import Dict, List, Tuple, Optional
from pathlib import Path
import csv
import math


CREATE_TABLE_RE = re.compile(r"CREATE\s+TABLE\s+`?([\w_]+)`?\s*\(", re.IGNORECASE)


def _extract_columns(block: str) -> Dict[str, str]:
    cols: Dict[str, str] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip().rstrip(',')
        if not line:
            continue
        # Skip constraints and keys
        up = line.upper()
        if up.startswith((
            'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'KEY', 'CONSTRAINT'
        )):
            continue
        # Expect backticked or bare identifier followed by type
        # Examples: `Id` INT PRIMARY KEY AUTO_INCREMENT
        m = re.match(r"`?([A-Za-z0-9_]+)`?\s+([A-Za-z]+)(?:\s*\([^)]*\))?", line)
        if not m:
            continue
        col, typ = m.group(1), m.group(2).upper()
        cols[col] = typ
    return cols


def parse_schema(schema_sql_path: str) -> Dict[str, Dict[str, str]]:
    with open(schema_sql_path, 'r', encoding='utf-8') as f:
        sql = f.read()
    tables: Dict[str, Dict[str, str]] = {}
    # Split by CREATE TABLE blocks
    pos = 0
    while True:
        m = CREATE_TABLE_RE.search(sql, pos)
        if not m:
            break
        table_name = m.group(1)
        start = m.end()
        # Find matching closing parenthesis followed by semicolon
        # We will scan until the first ")\n;" or ");"
        end = sql.find(');', start)
        if end == -1:
            # Fallback: look for ")\n" then semicolon
            end = sql.find(')\n', start)
            if end == -1:
                break
        block = sql[start:end]
        cols = _extract_columns(block)
        tables[table_name] = cols
        pos = end + 2
    return tables


def render_schema(tables: Dict[str, Dict[str, str]]) -> str:
    # Render with trailing commas per line (except last) to match prompt style.
    blocks: List[str] = []
    tnames = list(tables.keys())
    for i, tname in enumerate(tnames):
        cols = tables[tname]
        col_items = list(cols.items())
        lines = [f'"{tname}": {{']
        for j, (cname, ctype) in enumerate(col_items):
            comma = ',' if j < len(col_items) - 1 else ''
            lines.append(f'\t"{cname}": "{ctype}"{comma}')
        lines.append('}')
        blocks.append("\n".join(lines))
    return ",\n".join(blocks)


def collect_queries(dir_path: str) -> List[Tuple[int, str]]:
    items: List[Tuple[int, str]] = []
    for name in os.listdir(dir_path):
        m = re.match(r"query(\d+)\.sql$", name)
        if not m:
            continue
        idx = int(m.group(1))
        with open(os.path.join(dir_path, name), 'r', encoding='utf-8') as f:
            items.append((idx, f.read().rstrip()))
    items.sort(key=lambda x: x[0])
    return items


def _format_count(n: int) -> str:
    if n >= 1_000_000:
        v = n / 1_000_000.0
        s = f"{v:.1f}"
        if s.endswith('.0'):
            s = s[:-2]
        return f"{s} million"
    if n >= 1_000:
        v = n / 1_000.0
        s = f"{v:.0f}"
        return f"{s} thousand"
    return f"{n} rows"


def collect_row_counts(schema_dir: str, tables: Dict[str, Dict[str, str]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    base = Path(schema_dir)
    for t in tables.keys():
        # Case-sensitive and case-insensitive checks
        candidates = [base / f"{t}.csv", base / f"{t}.CSV", base / f"{t.lower()}.csv", base / f"{t.upper()}.csv"]
        csv_path: Optional[Path] = None
        for c in candidates:
            if c.exists():
                csv_path = c
                break
        if not csv_path:
            continue
        # Fast line count
        cnt = 0
        with csv_path.open('r', encoding='utf-8', newline='') as f:
            for _ in f:
                cnt += 1
        if cnt > 0:
            cnt -= 1  # header
        counts[t] = max(cnt, 0)
    return counts


def render_row_counts(counts: Dict[str, int], all_tables: List[str]) -> str:
    # Fill 0 for tables with no CSV found to ensure all tables are listed
    enriched = {t: counts.get(t, 0) for t in all_tables}
    ordered = sorted(enriched.items(), key=lambda x: x[1], reverse=True)
    lines: List[str] = []
    for t, n in ordered:
        lines.append(f'"{t}": {_format_count(n)};')
    return "\n".join(lines)


def render_queries(queries: List[Tuple[int, str]], latencies_raw: Optional[Dict[int, float]] = None) -> str:
    parts: List[str] = []
    # Expect latencies_raw values are in seconds already (load_latencies normalizes)
    as_seconds: Dict[int, float] = latencies_raw or {}
    for idx, body in queries:
        sec_str = "N/A"
        if as_seconds and idx in as_seconds:
            secs = float(as_seconds[idx])
            # Prefer integer seconds formatting like the sample; avoid 0 for sub-second values
            disp = int(round(secs))
            if secs > 0 and disp == 0:
                disp = 1
            sec_str = str(disp)
        parts.append(f"-- SQL{idx} : {sec_str} seconds --\n{body}\n\n")
    return "\n".join(parts).rstrip() + "\n"


def _parse_query_id(val: str) -> Optional[int]:
    if not val:
        return None
    val = str(val).strip()
    # Accept '7', 'query7', 'SQL7', etc.
    m = re.search(r"(\d+)$", val)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def load_latencies(default_base: Path, override_path: Optional[str], unit_override: str = "auto") -> Optional[Dict[int, float]]:
    path: Path
    if override_path:
        path = Path(override_path)
    else:
        path = default_base / "query_latency" / "latency_results.csv"
    if not path.exists():
        return None
    raw_map: Dict[int, float] = {}
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid_raw = row.get('query_id') or row.get('id') or row.get('query') or ''
            qid = _parse_query_id(qid_raw)
            if qid is None:
                continue
            val_raw = (
                row.get('elapsed_ms') or row.get('latency_ms') or row.get('ms') or
                row.get('elapsed_s') or row.get('elapsed_sec') or row.get('seconds') or row.get('sec') or row.get('s') or ''
            )
            try:
                val = float(val_raw)
            except Exception:
                continue
            raw_map[qid] = val
    if not raw_map:
        return None
    # Decide unit
    unit = unit_override or "auto"
    vals = list(raw_map.values())
    if unit == "auto":
        # Heuristic: if any value >= 1000, assume milliseconds; else assume seconds
        unit = 'ms' if any(v >= 1000.0 for v in vals) else 's'
    factor = 1.0/1000.0 if unit == 'ms' else 1.0
    norm: Dict[int, float] = {k: v*factor for k, v in raw_map.items()}
    return norm


def replace_fenced_block(md: str, anchor: str, new_code: str, fence_lang: str) -> str:
    # Find anchor
    anchor_pos = md.find(anchor)
    if anchor_pos == -1:
        raise ValueError(f"Anchor not found: {anchor}")
    # Find opening fence after anchor
    fence_start = md.find('~~~', anchor_pos)
    if fence_start == -1:
        raise ValueError("Opening fence not found after anchor")
    # Ensure we replace an opening line like ~~~sql (any suffix allowed)
    fence_line_end = md.find('\n', fence_start)
    if fence_line_end == -1:
        fence_line_end = len(md)
    # Find the closing fence '~~~' on its own line using multiline regex
    closing_re = re.compile(r'^\s*~~~\s*$', re.M)
    mclose = closing_re.search(md, pos=fence_line_end + 1)
    if not mclose:
        raise ValueError("Closing fence not found")
    close_start, close_end = mclose.start(), mclose.end()
    # Build new fenced block; ensure closing fence on its own line
    inner = new_code.rstrip('\n') + '\n'
    new_block = f"~~~{fence_lang}\n{inner}~~~"
    return md[:fence_start] + new_block + md[close_end:]


def main():
    p = argparse.ArgumentParser(description="Build prompt.md from schema.sql and queryN.sql")
    p.add_argument("schema_dir", help="Directory containing schema.sql and (optionally) CSVs")
    p.add_argument("--prompt", default="prompt.md", help="Path to prompt.md to update")
    p.add_argument("--sql-dir", default=None, help="Directory containing queryN.sql files; default: schema_dir")
    p.add_argument("--latency-csv", default=None, help="Path to latency_results.csv; default: ../query_latency/latency_results.csv relative to this script")
    p.add_argument("--latency-unit", choices=["auto","ms","s"], default="auto", help="Latency unit for CSV values (auto-detect by default)")
    args = p.parse_args()

    schema_sql = os.path.join(args.schema_dir, "schema.sql")
    if not os.path.isfile(schema_sql):
        raise SystemExit(f"schema.sql not found: {schema_sql}")

    tables = parse_schema(schema_sql)
    schema_block = render_schema(tables)

    # Row counts block (CSV-based if present)
    row_counts = collect_row_counts(args.schema_dir, tables)
    row_counts_block = render_row_counts(row_counts, list(tables.keys()))

    # Queries and optional latencies
    # Default latency path is ../query_latency/latency_results.csv relative to script location
    script_base = Path(__file__).resolve().parent.parent
    lat_map = load_latencies(script_base, args.latency_csv, args.latency_unit)
    sql_dir = args.sql_dir or args.schema_dir
    queries = collect_queries(sql_dir)
    queries_block = render_queries(queries, lat_map)

    with open(args.prompt, 'r', encoding='utf-8') as f:
        md = f.read()

    md = replace_fenced_block(md, "数据库当前的模式为：", schema_block, "sql")
    md = replace_fenced_block(md, "其中各表的行数从多到少分别为：", row_counts_block, "python")
    md = replace_fenced_block(md, "历史负载及其在当前模式下的执行时间为：", queries_block, "sql")

    with open(args.prompt, 'w', encoding='utf-8') as f:
        f.write(md)


if __name__ == "__main__":
    main()
