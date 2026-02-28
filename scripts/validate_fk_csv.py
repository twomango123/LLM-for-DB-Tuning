#!/usr/bin/env python3
import argparse
import csv
import os
import re
from collections import Counter, defaultdict


def read_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        return f.read()


def split_create_blocks(schema_sql: str):
    pat = re.compile(r"CREATE\s+TABLE\s+`([^`]+)`\s*\(", re.IGNORECASE)
    i = 0
    out = []
    while True:
        m = pat.search(schema_sql, i)
        if not m:
            break
        table = m.group(1)
        j = m.end()
        depth = 1
        in_str = False
        while j < len(schema_sql):
            ch = schema_sql[j]
            if in_str:
                if ch == '\\':
                    j += 2
                    continue
                if ch == "'":
                    in_str = False
            else:
                if ch == "'":
                    in_str = True
                elif ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        k = j + 1
                        while k < len(schema_sql) and schema_sql[k] != ';':
                            k += 1
                        block = schema_sql[m.start():k+1]
                        out.append((table, block))
                        i = k + 1
                        break
            j += 1
        else:
            break
    return out


def split_items(create_block: str):
    first = create_block.find('(')
    last = create_block.rfind(')')
    body = create_block[first+1:last]
    items = []
    buf = []
    depth = 0
    in_str = False
    i = 0
    while i < len(body):
        ch = body[i]
        if in_str:
            buf.append(ch)
            if ch == '\\':
                if i + 1 < len(body):
                    buf.append(body[i+1]); i += 2; continue
            elif ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True; buf.append(ch)
            elif ch == '(':
                depth += 1; buf.append(ch)
            elif ch == ')':
                depth -= 1; buf.append(ch)
            elif ch == ',' and depth == 0:
                s = ''.join(buf).strip()
                if s:
                    items.append(s)
                buf = []
            else:
                buf.append(ch)
        i += 1
    tail = ''.join(buf).strip()
    if tail:
        items.append(tail)
    return items


def parse_schema(schema_sql: str):
    blocks = split_create_blocks(schema_sql)
    pk = {}           # table -> [cols]
    fks = []          # (table, local_cols, ref_table, ref_cols)
    for table, block in blocks:
        items = split_items(block)
        # table-level PK
        pk_re = re.compile(r"^\s*PRIMARY\s+KEY\b.*\((.+)\)\s*$", re.IGNORECASE | re.DOTALL)
        fk_re = re.compile(r"^\s*FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+`([^`]+)`\s*\(([^)]+)\)", re.IGNORECASE | re.DOTALL)
        # column-level PK
        col_re = re.compile(r"^\s*`([^`]+)`\s+(.+)$")
        for it in items:
            m = pk_re.match(it)
            if m:
                inside = m.group(1)
                cols = re.findall(r"`([^`]+)`", inside)
                if cols:
                    pk[table] = cols
                continue
            m = fk_re.match(it)
            if m:
                l_inside, r_table, r_inside = m.groups()
                lcols = re.findall(r"`([^`]+)`", l_inside)
                rcols = re.findall(r"`([^`]+)`", r_inside)
                fks.append((table, lcols, r_table, rcols))
                continue
            m = col_re.match(it)
            if m:
                colname = m.group(1)
                rest = m.group(2)
                if re.search(r"\bPRIMARY\s+KEY\b", rest, re.IGNORECASE):
                    pk.setdefault(table, []).append(colname)
        # ensure uniqueness of pk list
        if table in pk:
            pk[table] = list(dict.fromkeys(pk[table]))
    return pk, fks


def load_csv(path):
    with open(path, 'r', encoding='utf-8', newline='') as f:
        rdr = csv.reader(f)
        rows = list(rdr)
    if not rows:
        return [], []
    return rows[0], rows[1:]


def main():
    ap = argparse.ArgumentParser(description='Validate PK uniqueness and FK integrity for expanded CSVs')
    ap.add_argument('--schema', required=True, help='schema.sql path')
    ap.add_argument('--dir', required=True, help='directory with CSVs')
    ap.add_argument('--suffix', default='_SF_5.csv', help='CSV filename suffix, default _SF_5.csv')
    args = ap.parse_args()

    schema_sql = read_file(args.schema)
    pk_map, fks = parse_schema(schema_sql)

    # Load all CSV headers/rows
    tables = {}
    for t in pk_map.keys() | {fk[0] for fk in fks} | {fk[2] for fk in fks}:
        csv_path = os.path.join(args.dir, f"{t}{args.suffix}")
        if os.path.isfile(csv_path):
            header, rows = load_csv(csv_path)
            tables[t] = (header, rows)

    # Check PK uniqueness for single-column PKs
    pk_dups = {}
    pk_sets = {}
    for t, cols in pk_map.items():
        if t not in tables:
            continue
        if len(cols) != 1:
            continue
        col = cols[0]
        header, rows = tables[t]
        if col not in header:
            continue
        idx = header.index(col)
        vals = [r[idx] for r in rows if idx < len(r) and r[idx] != '']
        cnt = Counter(vals)
        dups = [k for k, v in cnt.items() if v > 1]
        if dups:
            pk_dups[t] = len(dups)
        pk_sets[t] = set(vals)

    # Check FK integrity for single-column FKs
    fk_broken = defaultdict(int)
    for t, lcols, rt, rcols in fks:
        if len(lcols) != 1 or len(rcols) != 1:
            continue
        if t not in tables or rt not in pk_sets:
            continue
        lcol = lcols[0]
        header, rows = tables[t]
        if lcol not in header:
            continue
        li = header.index(lcol)
        refset = pk_sets[rt]
        miss = 0
        for r in rows:
            if li >= len(r):
                continue
            v = r[li]
            if v == '':
                continue
            if v not in refset:
                miss += 1
        if miss:
            fk_broken[(t, lcol, rt, rcols[0])] += miss

    print('PK duplicates per table (single-column PKs only):')
    if not pk_dups:
        print('  None')
    else:
        for t, n in pk_dups.items():
            print(f'  {t}: {n} duplicate key values')

    print('\nBroken FKs (single-column FKs only):')
    if not fk_broken:
        print('  None')
    else:
        for (t, l, rt, rc), n in fk_broken.items():
            print(f'  {t}.{l} -> {rt}.{rc}: {n} missing references')


if __name__ == '__main__':
    main()

