#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any

_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?[\w]+`?\.)?`?([\w_]+)`?)\s*\(",
    re.IGNORECASE,
)


def _strip_ident(s: str) -> str:
    s = s.strip()
    if (s.startswith('`') and s.endswith('`')) or (s.startswith('"') and s.endswith('"')):
        s = s[1:-1]
    return s


def _split_cols(s: str) -> List[str]:
    cols = []
    for part in s.split(','):
        c = _strip_ident(part.strip())
        if c:
            cols.append(c)
    return cols


def parse_schema_tables(schema_sql: Path) -> Tuple[Dict[str, List[str]], Dict[str, List[str]], List[Dict[str, Any]]]:
    sql = schema_sql.read_text(encoding='utf-8', errors='ignore')
    table_cols: Dict[str, List[str]] = {}
    table_pks: Dict[str, List[str]] = {}
    fks: List[Dict[str, Any]] = []
    pos = 0
    while True:
        m = _CREATE_TABLE_RE.search(sql, pos)
        if not m:
            break
        tname = m.group(1)
        start = m.end()
        end = sql.find(');', start)
        if end == -1:
            end = sql.find(')\n', start)
            if end == -1:
                break
        block = sql[start:end]
        cols: List[str] = []
        pks: List[str] = []

        for raw_line in block.splitlines():
            line = raw_line.strip().rstrip(',')
            if not line:
                continue
            up = line.upper()
            # PRIMARY KEY
            if up.startswith('PRIMARY KEY'):
                m_pk = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", line, re.IGNORECASE)
                if m_pk:
                    pks = _split_cols(m_pk.group(1))
                continue
            # FOREIGN KEY
            if 'FOREIGN KEY' in up and 'REFERENCES' in up:
                m_fk = re.search(
                    r"FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+`?([\w_]+)`?\s*\(([^)]+)\)",
                    line,
                    re.IGNORECASE,
                )
                if m_fk:
                    from_cols = _split_cols(m_fk.group(1))
                    ref_table = _strip_ident(m_fk.group(2))
                    ref_cols = _split_cols(m_fk.group(3))
                    fks.append({
                        'from_table': tname,
                        'from_columns': from_cols,
                        'to_table': ref_table,
                        'to_columns': ref_cols,
                        'source': 'schema',
                    })
                continue
            # column line
            if up.startswith(('UNIQUE', 'KEY', 'CONSTRAINT', 'CHECK')):
                continue
            m_col = re.match(r"`?([A-Za-z0-9_]+)`?\s+", line)
            if m_col:
                cols.append(m_col.group(1))

        table_cols[tname] = cols
        table_pks[tname] = pks
        pos = end + 2

    return table_cols, table_pks, fks


def _collect_aliases(sql: str) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    kw = r"WHERE|GROUP|ORDER|LIMIT|JOIN|LEFT|RIGHT|INNER|OUTER|ON|HAVING|UNION|EXCEPT|INTERSECT"
    for m in re.finditer(r"\bFROM\s+([`\w.]+)\s*(?:AS\s+)?(?!" + kw + r"\b)([`\w]+)?", sql, re.IGNORECASE):
        table = _strip_ident(m.group(1)).split('.')[-1]
        alias = _strip_ident(m.group(2)) if m.group(2) else table
        aliases[alias] = table
        aliases[table] = table
    for m in re.finditer(r"\bJOIN\s+([`\w.]+)\s*(?:AS\s+)?(?!" + kw + r"\b)([`\w]+)?", sql, re.IGNORECASE):
        table = _strip_ident(m.group(1)).split('.')[-1]
        alias = _strip_ident(m.group(2)) if m.group(2) else table
        aliases[alias] = table
        aliases[table] = table
    return aliases


def _extract_join_pairs(sql: str) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    # ON conditions
    for on in re.finditer(r"\bON\b(.+?)(?=\bJOIN\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\)|$)", sql, re.IGNORECASE | re.DOTALL):
        frag = on.group(1)
        for m in re.finditer(r"([`\w]+)\s*\.\s*([`\w]+)\s*=\s*([`\w]+)\s*\.\s*([`\w]+)", frag):
            left = f"{_strip_ident(m.group(1))}.{_strip_ident(m.group(2))}"
            right = f"{_strip_ident(m.group(3))}.{_strip_ident(m.group(4))}"
            pairs.append((left, right))
    # WHERE conditions
    for where in re.finditer(r"\bWHERE\b(.+?)(?=\bGROUP\b|\bORDER\b|\bLIMIT\b|\bUNION\b|\)|$)", sql, re.IGNORECASE | re.DOTALL):
        frag = where.group(1)
        for m in re.finditer(r"([`\w]+)\s*\.\s*([`\w]+)\s*=\s*([`\w]+)\s*\.\s*([`\w]+)", frag):
            left = f"{_strip_ident(m.group(1))}.{_strip_ident(m.group(2))}"
            right = f"{_strip_ident(m.group(3))}.{_strip_ident(m.group(4))}"
            pairs.append((left, right))
    # USING(col1, col2) with immediate previous table is too complex; skip for simplicity
    return pairs


def collect_query_joins(sql_dirs: List[Path], table_cols: Dict[str, List[str]]) -> Dict[Tuple[str, str, str, str], int]:
    counts: Dict[Tuple[str, str, str, str], int] = {}
    table_set = {t.lower() for t in table_cols.keys()}
    for d in sql_dirs:
        if not d.exists():
            continue
        for p in d.rglob('*.sql'):
            try:
                sql = p.read_text(encoding='utf-8', errors='ignore')
            except Exception:
                continue
            aliases = _collect_aliases(sql)
            for left, right in _extract_join_pairs(sql):
                if '.' not in left or '.' not in right:
                    continue
                l_alias, l_col = left.split('.', 1)
                r_alias, r_col = right.split('.', 1)
                lt = aliases.get(l_alias, l_alias)
                rt = aliases.get(r_alias, r_alias)
                if lt.lower() not in table_set or rt.lower() not in table_set:
                    continue
                # column existence check
                if l_col not in (table_cols.get(lt) or []) or r_col not in (table_cols.get(rt) or []):
                    continue
                key = (lt, l_col, rt, r_col)
                counts[key] = counts.get(key, 0) + 1
    return counts


def build_fk_from_joins(join_counts: Dict[Tuple[str, str, str, str], int], table_pks: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for (t1, c1, t2, c2), cnt in sorted(join_counts.items(), key=lambda x: (-x[1], x[0])):
        pk1 = set(table_pks.get(t1) or [])
        pk2 = set(table_pks.get(t2) or [])
        entry: Dict[str, Any]
        if c1 in pk1 and c2 not in pk2:
            entry = {
                'from_table': t2,
                'from_columns': [c2],
                'to_table': t1,
                'to_columns': [c1],
                'source': 'query',
                'count': cnt,
            }
        elif c2 in pk2 and c1 not in pk1:
            entry = {
                'from_table': t1,
                'from_columns': [c1],
                'to_table': t2,
                'to_columns': [c2],
                'source': 'query',
                'count': cnt,
            }
        else:
            # undirected heuristic fallback
            entry = {
                'from_table': t1,
                'from_columns': [c1],
                'to_table': t2,
                'to_columns': [c2],
                'source': 'query',
                'count': cnt,
                'direction': 'undirected',
            }
        out.append(entry)
    return out


def normalize_key(entry: Dict[str, Any]) -> Tuple:
    return (
        entry.get('from_table'),
        tuple(entry.get('from_columns') or []),
        entry.get('to_table'),
        tuple(entry.get('to_columns') or []),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description='Build foreign_keys.json from schema + query joins')
    ap.add_argument('--schema', required=True, help='schema.sql path')
    ap.add_argument('--sql-dir', action='append', default=[], help='SQL dir (can be repeated)')
    ap.add_argument('--out', required=True, help='output foreign_keys.json path')
    args = ap.parse_args()

    schema_path = Path(args.schema)
    if not schema_path.exists():
        raise SystemExit(f'schema not found: {schema_path}')

    table_cols, table_pks, fks_schema = parse_schema_tables(schema_path)

    sql_dirs = [Path(p) for p in (args.sql_dir or [])]
    join_counts = collect_query_joins(sql_dirs, table_cols)
    fks_query = build_fk_from_joins(join_counts, table_pks)

    # merge and dedupe
    seen = set()
    merged: List[Dict[str, Any]] = []
    for entry in fks_schema + fks_query:
        key = normalize_key(entry)
        if key in seen:
            continue
        seen.add(key)
        merged.append(entry)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'OK: wrote {out_path} ({len(merged)} entries)')


if __name__ == '__main__':
    main()
