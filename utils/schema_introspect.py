#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utilities to introspect MySQL schema using a DatabaseDriver-compatible object.

Functions here only rely on driver.execute_query() and return basic metadata
like column lists, primary keys, and foreign key mappings.
"""

from __future__ import annotations

from typing import Dict, List, Tuple, Optional


def get_table_columns(db, table: str) -> List[str]:
    sql = f"""
    SELECT COLUMN_NAME
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = '{table}'
    ORDER BY ORDINAL_POSITION
    """
    rows = db.execute_query(sql) if db else []
    return [r['COLUMN_NAME'] for r in rows]


def get_primary_key_columns(db, table: str) -> List[str]:
    sql = f"""
    SELECT k.COLUMN_NAME
    FROM information_schema.TABLE_CONSTRAINTS tc
    JOIN information_schema.KEY_COLUMN_USAGE k
      ON k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
     AND k.TABLE_NAME = tc.TABLE_NAME
     AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
      AND tc.TABLE_NAME = '{table}'
      AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    ORDER BY k.ORDINAL_POSITION
    """
    rows = db.execute_query(sql) if db else []
    return [r['COLUMN_NAME'] for r in rows]


def get_tables_columns(db, tables: List[str]) -> Dict[str, List[str]]:
    return {t: get_table_columns(db, t) for t in tables}


def find_fk_between(db, child_table: str, parent_table: str) -> List[Tuple[str, str]]:
    """Return list of (child_col, parent_col) where child_table FK references parent_table."""
    sql = f"""
    SELECT k.COLUMN_NAME AS child_col, k.REFERENCED_COLUMN_NAME AS parent_col
    FROM information_schema.KEY_COLUMN_USAGE k
    JOIN information_schema.TABLE_CONSTRAINTS tc
      ON tc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
     AND tc.TABLE_NAME = k.TABLE_NAME
     AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
    WHERE k.CONSTRAINT_SCHEMA = DATABASE()
      AND k.TABLE_NAME = '{child_table}'
      AND k.REFERENCED_TABLE_NAME = '{parent_table}'
      AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    ORDER BY k.ORDINAL_POSITION
    """
    rows = db.execute_query(sql) if db else []
    return [(r['child_col'], r['parent_col']) for r in rows]


def table_exists(db, table: str) -> bool:
    sql = f"""
    SELECT 1
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = '{table}'
    LIMIT 1
    """
    rows = db.execute_query(sql) if db else []
    return bool(rows)


def get_outbound_fks_info(db, table: str) -> List[Dict[str, List[str]]]:
    """Return outbound FK constraints with child column list.
    [{ 'name': str, 'columns': [child_col, ...] }]
    """
    sql_names = f"""
    SELECT DISTINCT tc.CONSTRAINT_NAME AS cname
    FROM information_schema.TABLE_CONSTRAINTS tc
    WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
      AND tc.TABLE_NAME = '{table}'
      AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    """
    names = [r['cname'] for r in (db.execute_query(sql_names) if db else [])]
    out: List[Dict[str, List[str]]] = []
    for cname in names:
        sql_cols = f"""
        SELECT COLUMN_NAME
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE CONSTRAINT_SCHEMA = DATABASE()
          AND TABLE_NAME = '{table}'
          AND CONSTRAINT_NAME = '{cname}'
        ORDER BY ORDINAL_POSITION
        """
        rows = db.execute_query(sql_cols) if db else []
        out.append({'name': cname, 'columns': [r['COLUMN_NAME'] for r in rows]})
    return out


def get_column_metadata(db, table: str, column: str) -> Dict[str, Optional[str]]:
    """Return basic metadata for a column from information_schema.COLUMNS.

    Keys:
      - data_type: lowercased DATA_TYPE (e.g., 'int', 'varchar', 'enum')
      - column_type: raw COLUMN_TYPE (e.g., "enum('a','b')", "int(11)")
      - is_nullable: 'YES' or 'NO'
    """
    sql = f"""
    SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = '{table}'
      AND COLUMN_NAME = '{column}'
    LIMIT 1
    """
    try:
        rows = db.execute_query(sql) if db else []
    except Exception:
        rows = []
    if not rows:
        return {'data_type': None, 'column_type': None, 'is_nullable': None}
    r = rows[0]
    return {
        'data_type': (r.get('DATA_TYPE') or '').lower() if isinstance(r, dict) else None,
        'column_type': r.get('COLUMN_TYPE') if isinstance(r, dict) else None,
        'is_nullable': r.get('IS_NULLABLE') if isinstance(r, dict) else None,
    }


def get_enum_values(db, table: str, column: str) -> List[str]:
    """If column is ENUM, return its declared values; otherwise empty list."""
    meta = get_column_metadata(db, table, column)
    ct = (meta.get('column_type') or '')
    dt = (meta.get('data_type') or '')
    if dt != 'enum' or not ct:
        return []
    # COLUMN_TYPE example: "enum('a','b','c')"
    s = ct.strip()
    try:
        i = s.index('(')
        j = s.rindex(')')
        inner = s[i + 1 : j]
    except ValueError:
        return []
    out: List[str] = []
    cur = []
    in_sq = False
    prev = ''
    for ch in inner:
        if ch == "'" and prev != '\\':
            in_sq = not in_sq
            # do not include quotes themselves
        elif ch == ',' and not in_sq:
            token = ''.join(cur).strip()
            if token.strip("' "):
                out.append(token.strip().strip("'"))
            cur = []
        else:
            cur.append(ch)
        prev = ch
    tail = ''.join(cur).strip()
    if tail.strip("' "):
        out.append(tail.strip().strip("'"))
    return out


def is_column_nullable(db, table: str, column: str) -> Optional[bool]:
    meta = get_column_metadata(db, table, column)
    is_nullable = meta.get('is_nullable')
    if is_nullable is None:
        return None
    s = str(is_nullable).upper()
    if s == 'YES':
        return True
    if s == 'NO':
        return False
    return None

