#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Utilities to introspect MySQL schema using a DatabaseDriver-compatible object.

Functions here only rely on driver.execute_query() and return basic metadata
like column lists, primary keys, and foreign key mappings.
"""

from __future__ import annotations

from typing import Dict, List, Tuple


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

