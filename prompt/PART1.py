#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
from typing import Dict, List, Tuple, Set, Optional


# 支持 schema.table 以及可选 IF NOT EXISTS，捕获最后的表名部分
_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?[\w]+`?\.)?`?([\w_]+)`?)\s*\(",
    re.IGNORECASE,
)

# 列定义：`col` TYPE(…)? [constraints...]
_COL_DEF_RE = re.compile(
    r"^`?([A-Za-z0-9_]+)`?\s+([A-Za-z]+)\s*(\([^)]*\))?(.*)$",
    re.IGNORECASE,
)


def _fixed_len_bytes(typ: str) -> int:
    t = typ.upper()
    if t in ("TINYINT",):
        return 1
    if t in ("SMALLINT", "YEAR"):
        return 2
    if t in ("MEDIUMINT",):
        return 3
    if t in ("INT", "INTEGER", "FLOAT"):
        return 4
    if t in ("BIGINT", "DOUBLE"):
        return 8
    if t in ("DATE",):
        return 3
    if t in ("TIME",):
        return 3
    if t in ("DATETIME", "TIMESTAMP"):
        return 8
    return 0


def _extract_columns_and_constraints(block: str) -> Tuple[Dict[str, Tuple[str, str]], Set[str], List[str]]:
    """解析列、主键、外键。

    返回：
      - columns: { column -> (TYPE(原样大写), length_repr) }
          length_repr: 对于带括号类型，直接使用括号内容（含括号）；
                       对于无括号但可估计固定字节的类型，返回 " [len=X]" 字样；
                       否则为空串。
      - pk_cols: 集合（包含行内 PRIMARY KEY 与表级 PRIMARY KEY 的列名）
      - fk_lines: 外键约束行（保持与 schema.sql 相同形式，去除收尾逗号）
    """
    cols: Dict[str, Tuple[str, str]] = {}
    pk_cols: Set[str] = set()
    fk_lines: List[str] = []

    for raw in block.splitlines():
        line = raw.strip().rstrip(',')
        if not line:
            continue
        up = line.upper()
        # 表级主键
        if up.startswith('PRIMARY KEY'):
            # PRIMARY KEY (`a`, `b`)
            m = re.search(r"\(([^)]*)\)", line)
            if m:
                inside = m.group(1)
                for part in inside.split(','):
                    name = part.strip().strip('`').strip()
                    if name:
                        pk_cols.add(name)
            continue
        # 外键
        if up.startswith('FOREIGN KEY'):
            fk_lines.append(line)
            continue
        # 其它约束行
        if up.startswith(('UNIQUE', 'KEY', 'CONSTRAINT')):
            # 捕获形如 CONSTRAINT xxx FOREIGN KEY (...) REFERENCES ...
            if up.startswith('CONSTRAINT') and 'FOREIGN KEY' in up:
                m_fk = re.search(r'(FOREIGN\s+KEY\b.*)$', line, flags=re.IGNORECASE)
                fk_lines.append(m_fk.group(1) if m_fk else line)
                continue
            continue
        # 列定义
        m = _COL_DEF_RE.match(line)
        if not m:
            continue
        cname = m.group(1)
        ctype = (m.group(2) or '').upper()
        paren = m.group(3) or ''  # 包含括号，如 (20) 或 (19,4)
        tail = (m.group(4) or '').upper()
        # 行内主键
        if 'PRIMARY KEY' in tail:
            pk_cols.add(cname)
        # 长度表示：若类型自带括号，则不重复；否则用固定字节估计
        length_repr = ''
        if not paren:
            fix = _fixed_len_bytes(ctype)
            if fix:
                length_repr = f" [len={fix}]"
        cols[cname] = (ctype + (paren or ''), length_repr)
    return cols, pk_cols, fk_lines


def _parse_fk_lines(fk_lines: List[str]) -> Dict[str, Tuple[str, str]]:
    """从外键行解析列 -> (ref_table, ref_col) 映射。
    仅处理单列外键。
    """
    m: Dict[str, Tuple[str, str]] = {}
    pat = re.compile(
        r"FOREIGN\s+KEY\s*\(\s*`?([A-Za-z0-9_]+)`?\s*\)\s*REFERENCES\s*`?([A-Za-z0-9_]+)`?\s*\(\s*`?([A-Za-z0-9_]+)`?\s*\)",
        re.IGNORECASE,
    )
    for line in fk_lines:
        mm = pat.search(line)
        if not mm:
            continue
        col = mm.group(1)
        ref_t = mm.group(2)
        ref_c = mm.group(3)
        m[col] = (ref_t, ref_c)
    return m


def _compute_len_value(ctype_full: str) -> Optional[int]:
    """估计列存储长度（近似字节数或限定长度）。
    - 对 CHAR/VARCHAR/BINARY/VARBINARY：使用括号中的第一个数
    - 对 DECIMAL/NUMERIC：按 MySQL 近似规则（每9位≈4字节，余数映射）
    - 无括号：使用固定字节映射（如 INT=4, DATETIME=8）
    无法解析时返回 None。
    """
    m = re.match(r"^([A-Z]+)\s*(\(([^)]*)\))?$", ctype_full.strip().upper())
    if not m:
        return None
    base = m.group(1)
    par = m.group(3)  # 无括号时为 None
    def _fixed_len_bytes_local(bt: str) -> Optional[int]:
        v = _fixed_len_bytes(bt)
        return v or None
    if par:
        # 括号中参数，取第一个数字作为长度
        parts = [p.strip() for p in par.split(',')]
        first_num = None
        for p in parts:
            mm2 = re.match(r"(\d+)", p)
            if mm2:
                first_num = int(mm2.group(1))
                break
        if base in ("CHAR", "VARCHAR", "BINARY", "VARBINARY") and first_num is not None:
            return first_num
        if base in ("DECIMAL", "NUMERIC") and first_num is not None:
            p = first_num
            groups = p // 9
            rem = p % 9
            rem_bytes_map = {0: 0, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 4, 8: 4}
            return groups * 4 + rem_bytes_map.get(rem, 0)
        # 其它带括号类型（如 BIT(n)），尝试返回第一个数
        if first_num is not None:
            return first_num
        return None
    # 无括号：固定长度类型
    return _fixed_len_bytes_local(base)


def parse_schema(schema_sql_path: str) -> Dict[str, Dict[str, object]]:
    with open(schema_sql_path, 'r', encoding='utf-8') as f:
        sql = f.read()
    tables: Dict[str, Dict[str, object]] = {}
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
        cols, pks, fks = _extract_columns_and_constraints(block)
        fk_map = _parse_fk_lines(fks)
        tables[tname] = {"columns": cols, "pks": pks, "fks": fks, "fk_map": fk_map}
        pos = end + 2
    return tables


def render_schema(tables: Dict[str, Dict[str, object]]) -> str:
    pieces: List[str] = []
    for tname, meta in tables.items():
        cols: Dict[str, Tuple[str, str]] = meta.get("columns", {})  # type: ignore
        pks: Set[str] = meta.get("pks", set())  # type: ignore
        fk_map: Dict[str, Tuple[str, str]] = meta.get("fk_map", {})  # type: ignore
        lines: List[str] = [f'"{tname}": {{']
        items = list(cols.items())
        for i, (cname, (ctype_full, _len_repr)) in enumerate(items):
            # 组装列属性集合 {"TYPE", "len=X", ["PRIMARY KEY"], ["FOREIGN KEY REFERENCES t(c)"]}
            type_base = re.match(r"^([A-Z]+)", ctype_full.upper()).group(1) if re.match(r"^([A-Z]+)", ctype_full.upper()) else ctype_full.upper()
            lens = _compute_len_value(ctype_full)
            attrs: List[str] = [f'"{type_base}"']
            if lens is not None:
                attrs.append(f'"len={lens}"')
            if cname in pks:
                attrs.append('"PRIMARY KEY"')
            if cname in fk_map:
                rt, rc = fk_map[cname]
                attrs.append(f'"FOREIGN KEY REFERENCES {rt}({rc})"')
            comma = ',' if i < len(items) - 1 else ''
            lines.append(f'\t"{cname}": ' + '{' + ", ".join(attrs) + '}' + f'{comma}')
        lines.append('}')
        pieces.append("\n".join(lines))
    return ",\n".join(pieces)


def build_part1(schema_sql_path: str) -> str:
    tables = parse_schema(schema_sql_path)
    if not tables:
        raise SystemExit(
            f"解析失败：未从 schema.sql 解析到任何表（路径：{schema_sql_path}）。请检查文件内容与 SQL 定义格式。"
        )
    schema_block = render_schema(tables)
    background = (
        "背景：\n\n"
        "你是一个数据库性能调优专家，需要进行数据库模式修改以提高系统的性能表现(降低查询延迟)。\n\n"
    )
    info = (
        "信息：\n\n"
        "数据库当前的模式为：\n\n"
        f"{schema_block}\n\n"
    )
    return background + info


def main() -> None:
    ap = argparse.ArgumentParser(description="PART1: 背景 + schema.sql 转换为模板片段")
    ap.add_argument("schema_sql", help="Path to schema.sql")
    ap.add_argument("--out", help="Output file; if omitted, prints to stdout")
    args = ap.parse_args()

    content = build_part1(args.schema_sql)
    if args.out:
        with open(args.out, 'w', encoding='utf-8') as f:
            f.write(content)
    else:
        print(content)


if __name__ == "__main__":
    main()
