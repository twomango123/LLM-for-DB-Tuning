#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import re
import csv
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import List, Tuple, Dict, Set, Optional

"""
该程序用于对数据进行扩充（按倍数复制并改写字段），原版以 SQL 转储
（包含 CREATE TABLE 与 INSERT）为输入并输出 SQL。现已新增对 CSV 的
输入与输出支持：

两种用法：
1) CSV -> CSV（推荐）：
   - 输入：CSV 文件（默认包含表头）。
   - 可选：--schema-sql 指定包含 CREATE TABLE 的 SQL，用于更准确的列类型识别；
           若不提供，则自动从 CSV 内容推断列类型（数值/时间/文本）。
   - 输出：扩充后的 CSV 文件。

2) 兼容旧版 SQL -> SQL：
   - 输入：包含 CREATE TABLE 与 INSERT 的 SQL 文件。
   - 输出：扩充后的 SQL 文件（保持兼容）。
"""

# 数值类型的属性的扩充规则是保留一个原属性值，其他行属性值按 (原值 * SF + k) 生成，k=1..SF-1
NUMERIC_TYPES = {
    "int", "integer", "bigint", "smallint", "tinyint", "mediumint",
    "decimal", "numeric", "float", "double", "real", "bit"
}
# 变长类型的属性的扩充规则是保留一个原属性值，其他行在最后一个数字字符后插入 k，若无数字则在末尾追加 k，k=1..SF-1
VARLEN_TYPES = {
    "varchar", "text", "tinytext", "mediumtext", "longtext",
    "blob", "tinyblob", "mediumblob", "longblob",
    "varbinary"
}
# 固定长度类型的属性若存在 UNIQUE 约束则报错终止，否则扩充的行都复制原属性值不变
FIXEDLEN_TYPES = {"char", "binary"}

# 时间类型的属性扩充的行均复制原属性值不变
TIME_TYPES = {"date", "datetime", "timestamp", "time", "year"}


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    base_type: str      
    full_type: str
    raw_def: str
    is_numeric: bool
    is_varlen: bool
    is_fixedlen: bool
    is_time: bool


@dataclass(frozen=True)
class ForeignKeySpec:
    local_cols: List[str]
    ref_table: str
    ref_cols: List[str]


@dataclass
class TableSchema:
    name: str
    columns: List[ColumnInfo]
    unique_cols: Set[str]
    pk_cols: List[str]
    foreign_keys: List[ForeignKeySpec]


# -----------------------------
# SQL parsing helpers
# -----------------------------
def _strip_sql_comments(sql: str) -> str:
    return sql


def parse_table_name(sql: str) -> str:
    m = re.search(r"CREATE\s+TABLE\s+`([^`]+)`", sql, flags=re.IGNORECASE)
    if not m:
        raise ValueError("未找到 CREATE TABLE `...` 语句，无法解析表名。")
    return m.group(1)


def extract_create_table_block(sql: str, table: str) -> Tuple[int, int, str]:
    """
    Return (start_idx, end_idx, block_text) for the CREATE TABLE ... ; block.
    """
    pat = re.compile(rf"CREATE\s+TABLE\s+`{re.escape(table)}`\s*\(", re.IGNORECASE)
    m = pat.search(sql)
    if not m:
        raise ValueError(f"未找到 CREATE TABLE `{table}` 的起始位置。")

    start = m.start()
    # Find the ending semicolon after the ENGINE=...; part
    semi = sql.find(";", m.end())
    if semi == -1:
        raise ValueError("CREATE TABLE 语句未以分号结尾，无法定位结束位置。")

    i = m.end()
    depth = 1  # we are after "("
    in_str = False
    while i < len(sql):
        ch = sql[i]
        if in_str:
            if ch == "\\":
                i += 2
                continue
            if ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    j = i + 1
                    while j < len(sql) and sql[j] != ";":
                        j += 1
                    if j >= len(sql):
                        raise ValueError("CREATE TABLE 语句未找到结尾分号 ';'。")
                    end = j + 1
                    return start, end, sql[start:end]
        i += 1

    raise ValueError("无法完整解析 CREATE TABLE 块。")


def parse_columns_and_uniques(create_block: str) -> Tuple[List[ColumnInfo], Set[str]]:
    """
    Parse columns in CREATE TABLE block and collect UNIQUE constrained columns.
    Supports:
      - column-level UNIQUE in column definition
      - table-level UNIQUE KEY ... (`col1`, `col2`)
    """
    # Get inside parentheses
    first_paren = create_block.find("(")
    last_paren = create_block.rfind(")")
    if first_paren == -1 or last_paren == -1 or last_paren <= first_paren:
        raise ValueError("CREATE TABLE 结构括号解析失败。")

    body = create_block[first_paren + 1:last_paren]

    # Split body into top-level comma-separated items (column defs, keys, constraints)
    items: List[str] = []
    buf = []
    depth = 0
    in_str = False
    i = 0
    while i < len(body):
        ch = body[i]
        if in_str:
            buf.append(ch)
            if ch == "\\":
                if i + 1 < len(body):
                    buf.append(body[i + 1])
                    i += 2
                    continue
            elif ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True
                buf.append(ch)
            elif ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                item = "".join(buf).strip()
                if item:
                    items.append(item)
                buf = []
            else:
                buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        items.append(tail)

    columns: List[ColumnInfo] = []
    unique_cols: Set[str] = set()

    # 1) Parse column definitions
    col_def_re = re.compile(r"^\s*`([^`]+)`\s+([a-zA-Z]+(?:\s*\([^)]*\))?)\s*(.*)$", re.DOTALL)
    for it in items:
        m = col_def_re.match(it)
        if not m:
            continue
        name = m.group(1)
        type_token = m.group(2).strip()
        rest = m.group(3) or ""
        base_type = re.match(r"^[a-zA-Z]+", type_token).group(0).lower()

        is_numeric = base_type in NUMERIC_TYPES
        is_varlen = base_type in VARLEN_TYPES
        is_fixedlen = base_type in FIXEDLEN_TYPES
        is_time = base_type in TIME_TYPES

        # column-level UNIQUE / PRIMARY KEY
        if re.search(r"\bUNIQUE\b", rest, flags=re.IGNORECASE):
            unique_cols.add(name)
        if re.search(r"\bPRIMARY\s+KEY\b", rest, flags=re.IGNORECASE):
            unique_cols.add(name)

        columns.append(ColumnInfo(
            name=name,
            base_type=base_type,
            full_type=type_token,
            raw_def=it,
            is_numeric=is_numeric,
            is_varlen=is_varlen,
            is_fixedlen=is_fixedlen,
            is_time=is_time
        ))

    if not columns:
        raise ValueError("未解析出任何列定义，请检查 CREATE TABLE 结构是否符合预期。")

    # 2) Parse table-level UNIQUE KEY
    # Example: UNIQUE KEY `email` (`email`)
    uniq_key_re = re.compile(r"^\s*UNIQUE\s+KEY\b.*\((.+)\)\s*$", re.IGNORECASE | re.DOTALL)
    for it in items:
        m = uniq_key_re.match(it)
        if not m:
            continue
        inside = m.group(1)
        cols = re.findall(r"`([^`]+)`", inside)
        for c in cols:
            unique_cols.add(c)

    # 3) Parse table-level PRIMARY KEY (treat as unique for our purposes)
    # Example: PRIMARY KEY (`product_id`)
    pk_re = re.compile(r"^\s*PRIMARY\s+KEY\b.*\((.+)\)\s*$", re.IGNORECASE | re.DOTALL)
    for it in items:
        m = pk_re.match(it)
        if not m:
            continue
        inside = m.group(1)
        cols = re.findall(r"`([^`]+)`", inside)
        # Only single-column PK is handled as a per-column unique here. Composite
        # PKs remain combination-unique; per-column uniqueness is not implied.
        if len(cols) == 1:
            unique_cols.add(cols[0])

    return columns, unique_cols


def _split_create_items(create_block: str) -> List[str]:
    first_paren = create_block.find("(")
    last_paren = create_block.rfind(")")
    if first_paren == -1 or last_paren == -1 or last_paren <= first_paren:
        raise ValueError("CREATE TABLE 结构括号解析失败。")

    body = create_block[first_paren + 1:last_paren]

    items: List[str] = []
    buf = []
    depth = 0
    in_str = False
    i = 0
    while i < len(body):
        ch = body[i]
        if in_str:
            buf.append(ch)
            if ch == "\\":
                if i + 1 < len(body):
                    buf.append(body[i + 1])
                    i += 2
                    continue
            elif ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True
                buf.append(ch)
            elif ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                item = "".join(buf).strip()
                if item:
                    items.append(item)
                buf = []
            else:
                buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        items.append(tail)
    return items


def parse_table_schema_from_create(create_block: str) -> TableSchema:
    m = re.search(r"CREATE\s+TABLE\s+`([^`]+)`", create_block, flags=re.IGNORECASE)
    if not m:
        raise ValueError("未在 CREATE TABLE 中找到表名。")
    table = m.group(1)

    items = _split_create_items(create_block)

    columns: List[ColumnInfo] = []
    unique_cols: Set[str] = set()
    pk_cols: List[str] = []

    col_def_re = re.compile(r"^\s*`([^`]+)`\s+([a-zA-Z]+(?:\s*\([^)]*\))?)\s*(.*)$", re.DOTALL)
    for it in items:
        m2 = col_def_re.match(it)
        if not m2:
            continue
        name = m2.group(1)
        type_token = m2.group(2).strip()
        rest = m2.group(3) or ""
        base_type = re.match(r"^[a-zA-Z]+", type_token).group(0).lower()

        is_numeric = base_type in NUMERIC_TYPES
        is_varlen = base_type in VARLEN_TYPES
        is_fixedlen = base_type in FIXEDLEN_TYPES
        is_time = base_type in TIME_TYPES

        if re.search(r"\bUNIQUE\b", rest, flags=re.IGNORECASE):
            unique_cols.add(name)
        if re.search(r"\bPRIMARY\s+KEY\b", rest, flags=re.IGNORECASE):
            pk_cols.append(name)
            unique_cols.add(name)

        columns.append(ColumnInfo(
            name=name,
            base_type=base_type,
            full_type=type_token,
            raw_def=it,
            is_numeric=is_numeric,
            is_varlen=is_varlen,
            is_fixedlen=is_fixedlen,
            is_time=is_time
        ))

    # UNIQUE KEY table-level
    uniq_key_re = re.compile(r"^\s*UNIQUE\s+KEY\b.*\((.+)\)\s*$", re.IGNORECASE | re.DOTALL)
    for it in items:
        m3 = uniq_key_re.match(it)
        if not m3:
            continue
        inside = m3.group(1)
        cols = re.findall(r"`([^`]+)`", inside)
        for c in cols:
            unique_cols.add(c)

    # PRIMARY KEY table-level
    pk_re = re.compile(r"^\s*PRIMARY\s+KEY\b.*\((.+)\)\s*$", re.IGNORECASE | re.DOTALL)
    for it in items:
        m4 = pk_re.match(it)
        if not m4:
            continue
        inside = m4.group(1)
        cols = re.findall(r"`([^`]+)`", inside)
        if cols:
            pk_cols = cols
            if len(cols) == 1:
                unique_cols.add(cols[0])

    # FOREIGN KEYs
    fk_re = re.compile(
        r"^\s*FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+`([^`]+)`\s*\(([^)]+)\)",
        re.IGNORECASE | re.DOTALL
    )
    fks: List[ForeignKeySpec] = []
    for it in items:
        m5 = fk_re.match(it)
        if not m5:
            continue
        local_inside = m5.group(1)
        ref_table = m5.group(2)
        ref_inside = m5.group(3)
        local_cols = re.findall(r"`([^`]+)`", local_inside)
        ref_cols = re.findall(r"`([^`]+)`", ref_inside)
        fks.append(ForeignKeySpec(local_cols=local_cols, ref_table=ref_table, ref_cols=ref_cols))

    return TableSchema(name=table, columns=columns, unique_cols=unique_cols, pk_cols=pk_cols, foreign_keys=fks)


def parse_all_table_schemas(schema_sql: str) -> Dict[str, TableSchema]:
    out: Dict[str, TableSchema] = {}
    pat = re.compile(r"CREATE\s+TABLE\s+`([^`]+)`\s*\(", re.IGNORECASE)
    pos = 0
    while True:
        m = pat.search(schema_sql, pos)
        if not m:
            break
        i = m.end()
        depth = 1
        in_str = False
        while i < len(schema_sql):
            ch = schema_sql[i]
            if in_str:
                if ch == "\\":
                    i += 2
                    continue
                if ch == "'":
                    in_str = False
            else:
                if ch == "'":
                    in_str = True
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        j = i + 1
                        while j < len(schema_sql) and schema_sql[j] != ";":
                            j += 1
                        if j >= len(schema_sql):
                            raise ValueError("CREATE TABLE 语句未找到结尾分号 ';'。")
                        end = j + 1
                        block = schema_sql[m.start():end]
                        ts = parse_table_schema_from_create(block)
                        out[ts.name] = ts
                        pos = end
                        break
            i += 1
        else:
            raise ValueError("无法完整解析某个 CREATE TABLE 块。")
    return out


def topo_sort_tables(schemas: Dict[str, TableSchema]) -> List[str]:
    adj: Dict[str, Set[str]] = {t: set() for t in schemas}
    indeg: Dict[str, int] = {t: 0 for t in schemas}
    for t, ts in schemas.items():
        for fk in ts.foreign_keys:
            if fk.ref_table in schemas:
                adj[fk.ref_table].add(t)
                indeg[t] += 1
    q = [t for t, d in indeg.items() if d == 0]
    order: List[str] = []
    while q:
        x = q.pop(0)
        order.append(x)
        for y in adj.get(x, []):
            indeg[y] -= 1
            if indeg[y] == 0:
                q.append(y)
    if len(order) != len(schemas):
        return list(schemas.keys())
    return order


def find_insert_block(sql: str, table: str) -> Tuple[int, int, str, str]:
    """
    Find INSERT INTO `table` VALUES ... ; block.
    Returns (start_idx, end_idx, insert_prefix, values_text)
    insert_prefix includes 'INSERT INTO `table` VALUES'
    values_text is the part after VALUES and before ending semicolon.
    """
    # Allow whitespace/newlines between tokens
    ins_re = re.compile(
        rf"(INSERT\s+INTO\s+`{re.escape(table)}`\s+VALUES)\s*",
        flags=re.IGNORECASE
    )
    m = ins_re.search(sql)
    if not m:
        raise ValueError(f"未找到 INSERT INTO `{table}` VALUES 语句。")

    start = m.start()
    prefix = m.group(1)

    # Scan until semicolon that ends this INSERT, respecting strings and parentheses depth.
    i = m.end()
    in_str = False
    depth = 0
    while i < len(sql):
        ch = sql[i]
        if in_str:
            if ch == "\\":
                i += 2
                continue
            if ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True
            elif ch == "(":
                depth += 1
            elif ch == ")":
                if depth > 0:
                    depth -= 1
            elif ch == ";" and depth == 0:
                end = i + 1
                values_text = sql[m.end():i].strip()
                return start, end, prefix, values_text
        i += 1

    raise ValueError("INSERT 语句未找到结束分号 ';'。")

def insert_after_last_digit(s: str, suffix: str) -> str:
    """
    在字符串 s 中最后一个数字字符(0-9)之后插入 suffix。
    若 s 中没有数字，则在末尾追加 suffix。
    例：
      "abc12def" + "3" => "abc123def"
      "abc" + "1" => "abc1"
      "abc9" + "2" => "abc92"
    """
    for i in range(len(s) - 1, -1, -1):
        if s[i].isdigit():
            return s[:i + 1] + suffix + s[i + 1:]
    return s + suffix

def parse_values_tuples(values_text: str) -> List[List[str]]:
    """
    Parse "(...),(...),(...)" into list of rows, each row is list of raw field strings.
    Keeps raw token forms like: 123, 'abc', NULL
    """
    rows: List[List[str]] = []
    i = 0
    n = len(values_text)

    def skip_ws(idx: int) -> int:
        while idx < n and values_text[idx].isspace():
            idx += 1
        return idx

    i = skip_ws(i)
    while i < n:
        i = skip_ws(i)
        if i >= n:
            break
        if values_text[i] != "(":
            # Sometimes dumps may have leading comments/spaces; try to find next '('
            nxt = values_text.find("(", i)
            if nxt == -1:
                break
            i = nxt
        i += 1  # consume '('

        fields: List[str] = []
        buf = []
        in_str = False

        while i < n:
            ch = values_text[i]
            if in_str:
                buf.append(ch)
                if ch == "\\":
                    if i + 1 < n:
                        buf.append(values_text[i + 1])
                        i += 2
                        continue
                elif ch == "'":
                    in_str = False
            else:
                if ch == "'":
                    in_str = True
                    buf.append(ch)
                elif ch == ",":
                    token = "".join(buf).strip()
                    fields.append(token)
                    buf = []
                elif ch == ")":
                    token = "".join(buf).strip()
                    fields.append(token)
                    buf = []
                    i += 1  # consume ')'
                    break
                else:
                    buf.append(ch)
            i += 1

        rows.append(fields)
        i = skip_ws(i)
        if i < n and values_text[i] == ",":
            i += 1
            continue
        else:
            # End of tuple list
            break

    return rows


# -----------------------------
# Value transformation helpers
# -----------------------------
def is_null_token(tok: str) -> bool:
    return tok.strip().upper() == "NULL"


def unquote_sql_string(tok: str) -> str:
    """
    tok should be a single-quoted SQL string like 'abc\'d'
    Return the unescaped python string.
    """
    tok = tok.strip()
    if len(tok) >= 2 and tok[0] == "'" and tok[-1] == "'":
        s = tok[1:-1]
        # MySQL dump commonly uses backslash escaping
        s = s.replace("\\'", "'").replace("\\\\", "\\")
        return s
    # If not quoted, return as-is
    return tok


def quote_sql_string(s: str) -> str:
    s = s.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def parse_numeric(tok: str) -> Decimal:
    tok = tok.strip()
    # Decimal supports int/float-like tokens
    return Decimal(tok)


def format_decimal(d: Decimal) -> str:
    # Avoid scientific notation for integers / simple decimals
    s = format(d, "f")
    # Trim trailing zeros if decimal
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s if s else "0"


def expand_rows(
    rows: List[List[str]],
    columns: List[ColumnInfo],
    unique_cols: Set[str],
    sf: int
) -> List[List[str]]:
    if sf <= 0:
        raise ValueError("SF 必须是正整数。")

    col_count = len(columns)
    for r in rows:
        if len(r) != col_count:
            raise ValueError(f"INSERT 行字段数({len(r)})与表列数({col_count})不一致：{r}")

    # Check fixed-length char UNIQUE constraint
    for col in columns:
        if col.is_fixedlen and (col.name in unique_cols):
            raise RuntimeError(
                f"列 `{col.name}` 为固定长度类型({col.full_type})且存在 UNIQUE 约束，按规则无法扩充，已终止。"
            )

    # For numeric columns that must be unique (including PRIMARY KEY),
    # compute a non-colliding offset so that new values never collide with
    # any original values: new_val = orig + k * (max-min+1)
    numeric_unique_offsets: Dict[int, Decimal] = {}
    for idx, col in enumerate(columns):
        if col.is_numeric and (col.name in unique_cols):
            min_v = None
            max_v = None
            for r in rows:
                tok = r[idx]
                if is_null_token(tok):
                    continue
                try:
                    d = parse_numeric(tok)
                except InvalidOperation:
                    continue
                min_v = d if min_v is None else (d if d < min_v else min_v)
                max_v = d if max_v is None else (d if d > max_v else max_v)
            if min_v is not None and max_v is not None:
                delta = (max_v - min_v) + Decimal(1)
                if delta <= 0:
                    delta = Decimal(1)
                numeric_unique_offsets[idx] = delta

    out: List[List[str]] = []
    for r in rows:
        for k in range(sf):
            new_row: List[str] = []
            for tok, col in zip(r, columns):
                if is_null_token(tok):
                    new_row.append(tok.strip())
                    continue

                if col.is_numeric:
                    d = parse_numeric(tok)
                    new_val = d
                    if k != 0:
                        if columns.index(col) in numeric_unique_offsets:
                            off = numeric_unique_offsets[columns.index(col)]
                            new_val = d + (off * k)
                        else:
                            new_val = d * sf + k  # 原有规则（非唯一列）
                    new_row.append(format_decimal(new_val))
                elif col.is_varlen:
                    s = unquote_sql_string(tok)
                    if k == 0:
                        s2 = s  # 保留一个原属性值
                    else:
                        s2 = insert_after_last_digit(s, str(k))  # 在最后一个数字后插入 k；无数字则末尾追加
                    new_row.append(quote_sql_string(s2))
                elif col.is_fixedlen:
                    # no UNIQUE => unchanged
                    new_row.append(tok.strip())
                elif col.is_time:
                    new_row.append(tok.strip())
                else:
                    # Unknown types: be conservative: copy unchanged
                    new_row.append(tok.strip())
            out.append(new_row)

    return out


def rebuild_insert_sql(table: str, prefix: str, expanded: List[List[str]]) -> str:
    lines = []
    lines.append(f"{prefix}\n")
    for i, row in enumerate(expanded):
        tup = "(" + ",".join(row) + ")"
        if i == len(expanded) - 1:
            lines.append(tup + ";\n")
        else:
            lines.append(tup + ",\n")
    return "".join(lines)


def update_auto_increment_in_create(create_block: str, new_auto: int) -> str:
    # Replace AUTO_INCREMENT=xxxx in the table options (after ) ENGINE=...)
    return re.sub(
        r"(AUTO_INCREMENT\s*=\s*)\d+",
        rf"\g<1>{new_auto}",
        create_block,
        flags=re.IGNORECASE
    )


def compute_new_auto_increment(rows: List[List[str]], columns: List[ColumnInfo], sf: int) -> Optional[int]:
    # Heuristic: if first column is numeric and named 'id', use it
    idx = None
    for i, c in enumerate(columns):
        if c.name.lower() == "id" and c.is_numeric:
            idx = i
            break
    if idx is None:
        return None

    max_id = None
    for r in rows:
        tok = r[idx]
        if is_null_token(tok):
            continue
        try:
            d = parse_numeric(tok)
        except InvalidOperation:
            continue
        max_id = d if max_id is None else max(max_id, d)

    if max_id is None:
        return None

    # After expansion: max becomes max_id*sf + (sf-1)
    new_max = max_id * sf + (sf - 1)
    # AUTO_INCREMENT should be integer; if not integer, skip
    if new_max != new_max.to_integral_value():
        return None
    return int(new_max) + 1


# -----------------------------
# CSV helpers and expansion
# -----------------------------
def looks_like_datetime(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    # yyyy-mm-dd or yyyy-mm-dd hh:mm:ss
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return True
    if re.match(r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$", s):
        return True
    # hh:mm:ss
    if re.match(r"^\d{2}:\d{2}:\d{2}$", s):
        return True
    # year only
    if re.match(r"^\d{4}$", s):
        return True
    return False


def infer_columns_from_csv(header: List[str], rows: List[List[str]]) -> List[ColumnInfo]:
    """
    Best-effort type inference from CSV values:
      - numeric if all non-empty/nonnull parse as Decimal
      - time if values look like date/datetime/time
      - else treat as varlen text
    Unique constraints are not inferred here.
    """
    n_cols = len(header)
    cols: List[ColumnInfo] = []
    for c in range(n_cols):
        values = [r[c] for r in rows if c < len(r)]
        nonnull = [v for v in values if v is not None and str(v).strip() != ""]
        is_numeric = False
        is_time = False
        if nonnull:
            all_numeric = True
            for v in nonnull:
                try:
                    _ = Decimal(str(v).strip())
                except Exception:
                    all_numeric = False
                    break
            if all_numeric:
                is_numeric = True
            else:
                # if not numeric, consider datetime-like
                samples = nonnull[:50]
                if samples and all(looks_like_datetime(str(v)) for v in samples):
                    is_time = True

        base_type = "int" if is_numeric else ("datetime" if is_time else "varchar")
        cols.append(ColumnInfo(
            name=header[c] if c < len(header) else f"col{c+1}",
            base_type=base_type,
            full_type=base_type,
            raw_def=base_type,
            is_numeric=is_numeric,
            is_varlen=(not is_numeric and not is_time),
            is_fixedlen=False,
            is_time=is_time,
        ))
    return cols


def read_csv(input_csv: str, delimiter: str = ",", no_header: bool = False) -> Tuple[List[str], List[List[str]]]:
    with open(input_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        all_rows = list(reader)

    if not all_rows:
        return [], []

    if no_header:
        header = [f"col{i+1}" for i in range(len(all_rows[0]))]
        data_rows = all_rows
    else:
        header = all_rows[0]
        data_rows = all_rows[1:]
    return header, data_rows


def write_csv(output_csv: str, header: List[str], rows: List[List[Optional[str]]], delimiter: str = ",") -> None:
    os.makedirs(os.path.dirname(os.path.abspath(output_csv)) or ".", exist_ok=True)
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=delimiter)
        if header:
            writer.writerow(header)
        for r in rows:
            writer.writerow(["" if (v is None or (isinstance(v, str) and v.upper() == "NULL")) else v for v in r])


def align_columns_to_header(columns: List[ColumnInfo], header: List[str]) -> List[ColumnInfo]:
    """
    Reorder columns to match CSV header when names align. If not all names match,
    fall back to original order.
    """
    name_to_col = {c.name: c for c in columns}
    if all(h in name_to_col for h in header):
        return [name_to_col[h] for h in header]
    return columns


def expand_rows_csv(
    rows: List[List[str]],
    columns: List[ColumnInfo],
    unique_cols: Set[str],
    sf: int
) -> List[List[str]]:
    if sf <= 0:
        raise ValueError("SF 必须是正整数。")

    col_count = len(columns)
    for r in rows:
        if len(r) != col_count:
            raise ValueError(f"CSV 行字段数({len(r)})与表列数({col_count})不一致：{r}")

    for col in columns:
        if col.is_fixedlen and (col.name in unique_cols):
            raise RuntimeError(
                f"列 `{col.name}` 为固定长度类型({col.full_type})且存在 UNIQUE 约束，按规则无法扩充，已终止。"
            )

    # For numeric columns that must be unique (including PRIMARY KEY),
    # compute a non-colliding offset: new_val = orig + k * (max-min+1)
    numeric_unique_offsets: Dict[int, Decimal] = {}
    for idx, col in enumerate(columns):
        if col.is_numeric and (col.name in unique_cols):
            min_v = None
            max_v = None
            for r in rows:
                if idx >= len(r):
                    continue
                raw = r[idx]
                if raw is None:
                    continue
                s = str(raw).strip()
                if s == "" or s.upper() == "NULL":
                    continue
                try:
                    d = Decimal(s)
                except Exception:
                    continue
                min_v = d if min_v is None else (d if d < min_v else min_v)
                max_v = d if max_v is None else (d if d > max_v else max_v)
            if min_v is not None and max_v is not None:
                delta = (max_v - min_v) + Decimal(1)
                if delta <= 0:
                    delta = Decimal(1)
                numeric_unique_offsets[idx] = delta

    out: List[List[str]] = []
    for r in rows:
        for k in range(sf):
            new_row: List[str] = []
            for val, col in zip(r, columns):
                raw = "" if val is None else str(val)
                is_null = (raw.strip() == "" or raw.strip().upper() == "NULL")

                if is_null:
                    new_row.append("")
                    continue

                if col.is_numeric:
                    d = Decimal(raw.strip())
                    new_val = d
                    if k != 0:
                        idx = columns.index(col)
                        if idx in numeric_unique_offsets:
                            off = numeric_unique_offsets[idx]
                            new_val = d + (off * k)
                        else:
                            new_val = d * sf + k
                    new_row.append(format_decimal(new_val))
                elif col.is_varlen:
                    s = raw
                    if k == 0:
                        s2 = s
                    else:
                        s2 = insert_after_last_digit(s, str(k))
                    new_row.append(s2)
                else:
                    # fixed/time/unknown -> unchanged
                    new_row.append(raw)
            out.append(new_row)
    return out


# -----------------------------
# Bulk dataset CSV expansion with FK-preserving mapping
# -----------------------------
def _find_table_csv(input_dir: str, table: str) -> str:
    cand = os.path.join(input_dir, f"{table}.csv")
    if os.path.isfile(cand):
        return cand
    cand2 = os.path.join(input_dir, f"{table.lower()}.csv")
    if os.path.isfile(cand2):
        return cand2
    cand3 = os.path.join(input_dir, f"{table.upper()}.csv")
    if os.path.isfile(cand3):
        return cand3
    # Last resort: case-insensitive search by stem
    t_lower = table.lower()
    for fn in os.listdir(input_dir):
        if fn.lower().endswith('.csv') and os.path.splitext(fn)[0].lower() == t_lower:
            return os.path.join(input_dir, fn)
    return ""


def expand_dataset_csv(input_dir: str, schema_sql_path: str, sf: int, output_dir: str, delimiter: str = ",") -> None:
    with open(schema_sql_path, "r", encoding="utf-8") as f:
        schema_sql = _strip_sql_comments(f.read())

    schemas = parse_all_table_schemas(schema_sql)
    order = topo_sort_tables(schemas)

    os.makedirs(output_dir, exist_ok=True)

    # Cache table CSVs
    table_rows_cache: Dict[str, Tuple[List[str], List[List[str]]]] = {}
    for t in order:
        csv_path = _find_table_csv(input_dir, t)
        if not csv_path:
            table_rows_cache[t] = ([], [])
            continue
        header, rows = read_csv(csv_path, delimiter=delimiter, no_header=False)
        table_rows_cache[t] = (header, rows)

    # Precompute primary key mappings for propagation to FKs
    pk_numeric_delta: Dict[str, Decimal] = {}
    pk_is_varlen: Set[str] = set()
    for t in order:
        ts = schemas[t]
        header, rows = table_rows_cache.get(t, ([], []))
        if not rows or not header:
            continue
        if len(ts.pk_cols) != 1:
            continue  # only single-column PK supported for mapping
        pk_name = ts.pk_cols[0]
        # Align columns to header
        cols, _uniq = parse_columns_and_uniques(extract_create_table_block(schema_sql, t)[2])
        cols = align_columns_to_header(cols, header)
        # Locate PK index
        try:
            pk_idx = header.index(pk_name)
        except ValueError:
            # Fallback by ColumnInfo name
            pk_idx = None
            for i, c in enumerate(cols):
                if c.name == pk_name:
                    pk_idx = i
                    break
            if pk_idx is None:
                continue
        pk_colinfo = cols[pk_idx]
        if pk_colinfo.is_numeric:
            min_v = None
            max_v = None
            for r in rows:
                if pk_idx >= len(r):
                    continue
                raw = r[pk_idx]
                if raw is None:
                    continue
                s = str(raw).strip()
                if s == "" or s.upper() == "NULL":
                    continue
                try:
                    d = Decimal(s)
                except Exception:
                    continue
                min_v = d if min_v is None else (d if d < min_v else min_v)
                max_v = d if max_v is None else (d if d > max_v else max_v)
            if min_v is not None and max_v is not None:
                delta = (max_v - min_v) + Decimal(1)
                if delta <= 0:
                    delta = Decimal(1)
                pk_numeric_delta[t] = delta
        elif pk_colinfo.is_varlen:
            pk_is_varlen.add(t)

    # Expand each table with FK-aware mapping
    for t in order:
        ts = schemas[t]
        header, rows = table_rows_cache.get(t, ([], []))
        if not rows or not header:
            continue
        # Align ColumnInfo to header order
        cols, uniq = parse_columns_and_uniques(extract_create_table_block(schema_sql, t)[2])
        cols = align_columns_to_header(cols, header)

        # Map FK local column index -> referenced table (single-column only)
        fk_col_map: Dict[int, str] = {}
        for fk in ts.foreign_keys:
            if len(fk.local_cols) == 1 and len(fk.ref_cols) == 1:
                local_col = fk.local_cols[0]
                # Obtain index by header name
                try:
                    idx = header.index(local_col)
                except ValueError:
                    idx = None
                    for i, c in enumerate(cols):
                        if c.name == local_col:
                            idx = i
                            break
                if idx is not None:
                    fk_col_map[idx] = fk.ref_table

        # Precompute per-column numeric unique offsets for this table
        numeric_unique_offsets: Dict[int, Decimal] = {}
        for idx, c in enumerate(cols):
            if c.is_numeric and (c.name in uniq):
                min_v = None
                max_v = None
                for r in rows:
                    if idx >= len(r):
                        continue
                    raw = r[idx]
                    if raw is None:
                        continue
                    s = str(raw).strip()
                    if s == "" or s.upper() == "NULL":
                        continue
                    try:
                        d = Decimal(s)
                    except Exception:
                        continue
                    min_v = d if min_v is None else (d if d < min_v else min_v)
                    max_v = d if max_v is None else (d if d > max_v else max_v)
                if min_v is not None and max_v is not None:
                    delta = (max_v - min_v) + Decimal(1)
                    if delta <= 0:
                        delta = Decimal(1)
                    numeric_unique_offsets[idx] = delta

        # Locate PK index for this table if single-column
        pk_idx: Optional[int] = None
        if len(ts.pk_cols) == 1:
            pk_name = ts.pk_cols[0]
            try:
                pk_idx = header.index(pk_name)
            except ValueError:
                for i, c in enumerate(cols):
                    if c.name == pk_name:
                        pk_idx = i
                        break

        expanded: List[List[str]] = []
        for r in rows:
            for k in range(sf):
                new_row: List[str] = []
                for j, (val, col) in enumerate(zip(r, cols)):
                    raw = "" if val is None else str(val)
                    is_null = (raw.strip() == "" or raw.strip().upper() == "NULL")
                    if is_null:
                        new_row.append("")
                        continue

                    # FK propagation takes precedence
                    if j in fk_col_map:
                        ref_table = fk_col_map[j]
                        if ref_table in pk_numeric_delta and col.is_numeric:
                            d = Decimal(raw.strip())
                            if k == 0:
                                new_row.append(format_decimal(d))
                            else:
                                off = pk_numeric_delta[ref_table]
                                new_row.append(format_decimal(d + off * k))
                            continue
                        if ref_table in pk_is_varlen and col.is_varlen:
                            s = raw
                            if k == 0:
                                new_row.append(s)
                            else:
                                new_row.append(insert_after_last_digit(s, str(k)))
                            continue
                        # If referenced table has unknown PK transform, fall back to default below

                    if col.is_numeric:
                        d = Decimal(raw.strip())
                        if k == 0:
                            new_row.append(format_decimal(d))
                        else:
                            # Prefer PK delta if this is the PK column
                            if pk_idx is not None and j == pk_idx and (t in pk_numeric_delta):
                                off = pk_numeric_delta[t]
                                new_row.append(format_decimal(d + off * k))
                            elif j in numeric_unique_offsets:
                                off = numeric_unique_offsets[j]
                                new_row.append(format_decimal(d + off * k))
                            else:
                                new_row.append(format_decimal(d * sf + k))
                    elif col.is_varlen:
                        s = raw
                        if k == 0:
                            new_row.append(s)
                        else:
                            new_row.append(insert_after_last_digit(s, str(k)))
                    else:
                        new_row.append(raw)
                expanded.append(new_row)

        out_name = f"{t}_SF_{sf}.csv"
        out_path = os.path.join(output_dir, out_name)
        write_csv(out_path, header, expanded, delimiter=delimiter)
        print(f"OK: 生成 {t} -> {out_path} (原始行数 {len(rows)}, 扩充后 {len(expanded)})")

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="按规则进行数据扩充，支持 SQL 或 CSV 输入/输出。")
    ap.add_argument("input_path", help="输入文件路径（.sql 或 .csv），或数据集目录（bulk 模式）")
    ap.add_argument("SF", type=int, help="数据量扩充倍数 SF（正整数）")
    ap.add_argument("output_path", nargs="?", default="", help="输出路径（为空则当前目录；CSV/SQL 文件路径或目录）")
    # CSV 相关
    ap.add_argument("--delimiter", default=",", help="CSV 分隔符，默认 ,")
    ap.add_argument("--no-header", action="store_true", help="输入 CSV 无表头")
    ap.add_argument("--schema-sql", default="", help="可选：提供 CREATE TABLE 的 SQL 以准确识别列类型")
    args = ap.parse_args()

    input_path = args.input_path
    sf = args.SF
    out_path_arg = args.output_path.strip()

    if sf <= 0:
        raise SystemExit("SF 必须为正整数。")

    # Bulk dataset mode: input_path is a directory containing multiple CSVs,
    # and --schema-sql specifies full schema with all CREATE TABLEs
    if os.path.isdir(input_path):
        if not args.schema_sql:
            raise SystemExit("目录批量扩充需要提供 --schema-sql（包含所有表的 CREATE TABLE）")
        out_dir = os.path.abspath(out_path_arg or input_path)
        with open(args.schema_sql, "r", encoding="utf-8") as f:
            _ = f.read()  # quick validation of path; actual read in callee
        # Perform dataset expansion preserving外键一致性
        expand_dataset_csv(input_path, args.schema_sql, sf, out_dir, delimiter=args.delimiter)
        print(f"OK: 目录批量扩充完成 -> {out_dir}")
        return

    if not os.path.isfile(input_path):
        raise SystemExit(f"输入文件不存在：{input_path}")

    base_name = os.path.basename(input_path)
    base, ext = os.path.splitext(base_name)
    ext_lower = ext.lower()

    if ext_lower == ".csv":
        # CSV -> CSV
        header, data_rows = read_csv(input_path, delimiter=args.delimiter, no_header=args.no_header)

        columns: List[ColumnInfo]
        unique_cols: Set[str] = set()

        if args.schema_sql:
            with open(args.schema_sql, "r", encoding="utf-8") as f:
                schema_sql = _strip_sql_comments(f.read())
            table = parse_table_name(schema_sql)
            _, _, create_block = extract_create_table_block(schema_sql, table)
            columns, unique_cols = parse_columns_and_uniques(create_block)
            if header:
                columns = align_columns_to_header(columns, header)
        else:
            # Infer from CSV content
            columns = infer_columns_from_csv(header, data_rows)

        expanded_rows = expand_rows_csv(data_rows, columns, unique_cols, sf)

        # Output naming
        out_file_name = f"{base}_SF_{sf}.csv"
        if out_path_arg == "":
            out_dir = os.getcwd()
            out_full_path = os.path.join(out_dir, out_file_name)
        else:
            if out_path_arg.lower().endswith(".csv"):
                out_full_path = os.path.abspath(out_path_arg)
            else:
                out_dir = os.path.abspath(out_path_arg)
                os.makedirs(out_dir, exist_ok=True)
                out_full_path = os.path.join(out_dir, out_file_name)

        write_csv(out_full_path, header if not args.no_header else [], expanded_rows, delimiter=args.delimiter)

        print(f"OK: 已生成扩充后的 CSV 文件：{out_full_path}")
        print(f"原始行数：{len(data_rows)}  扩充后行数：{len(expanded_rows)}  SF={sf}")

    else:
        # 兼容旧版 SQL -> SQL
        with open(input_path, "r", encoding="utf-8") as f:
            sql = f.read()

        sql = _strip_sql_comments(sql)
        table = parse_table_name(sql)

        # Parse CREATE TABLE info
        create_start, create_end, create_block = extract_create_table_block(sql, table)
        columns, unique_cols = parse_columns_and_uniques(create_block)

        # Parse INSERT block
        ins_start, ins_end, prefix, values_text = find_insert_block(sql, table)
        rows = parse_values_tuples(values_text)

        expanded_rows = expand_rows(rows, columns, unique_cols, sf)

        # Optionally update AUTO_INCREMENT in CREATE TABLE options
        new_auto = compute_new_auto_increment(rows, columns, sf)
        if new_auto is not None:
            new_create_block = update_auto_increment_in_create(create_block, new_auto)
            sql = sql[:create_start] + new_create_block + sql[create_end:]
            # If we changed create block length, adjust insert indices by re-finding insert block in updated sql
            ins_start, ins_end, prefix, values_text = find_insert_block(sql, table)

        new_insert_sql = rebuild_insert_sql(table, prefix, expanded_rows)

        new_sql = sql[:ins_start] + new_insert_sql + sql[ins_end:]

        # Output file naming
        out_file_name = f"{base}_SF_{sf}.sql"

        # Output path handling
        if out_path_arg == "":
            out_dir = os.getcwd()
            out_full_path = os.path.join(out_dir, out_file_name)
        else:
            if out_path_arg.lower().endswith(".sql"):
                out_full_path = os.path.abspath(out_path_arg)
            else:
                out_dir = os.path.abspath(out_path_arg)
                os.makedirs(out_dir, exist_ok=True)
                out_full_path = os.path.join(out_dir, out_file_name)

        with open(out_full_path, "w", encoding="utf-8") as f:
            f.write(new_sql)

        print(f"OK: 已生成扩充后的 SQL 文件：{out_full_path}")
        print(f"表：{table}  原始行数：{len(rows)}  扩充后行数：{len(expanded_rows)}  SF={sf}")


if __name__ == "__main__":
    main()
