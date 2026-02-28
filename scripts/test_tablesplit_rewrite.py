#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Minimal tests for TableSplit apply_to_sql (read) and apply_to_write_sql (write).

Run: python3 scripts/test_tablesplit_rewrite.py
"""

from __future__ import annotations

import sys
import os

ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from rewrite.TableSplit import TableSplit


def _norm(s: str) -> str:
    return ' '.join((s or '').replace('\n', ' ').replace('\t', ' ').split())

def _flat(s: str) -> str:
    return _norm(s).replace('`','').replace(' ','')


def test_read_rewrite() -> None:
    ts = TableSplit(
        old_table='products',
        new_tables=['products_core', 'products_desc'],
        columnList={
            'products_core': ['product_id', 'product_name', 'product_price'],
            'products_desc': ['product_id', 'product_description'],
        },
        primary_keys_dict={'products_core': ['product_id'], 'products_desc': ['product_id']},
        new_view='view_products',
        is_retained=False,
    )

    src = "SELECT product_name FROM products WHERE product_id = 1;"
    out = ts.apply_to_sql(src)
    assert 'FROM view_products' in _norm(out), f"read rewrite failed: {out}"


def test_write_insert_split() -> None:
    ts = TableSplit(
        old_table='products',
        new_tables=['products_core', 'products_desc'],
        columnList={
            'products_core': ['product_id', 'product_name', 'product_price'],
            'products_desc': ['product_id', 'product_description'],
        },
        primary_keys_dict={'products_core': ['product_id'], 'products_desc': ['product_id']},
        new_view='view_products',
        is_retained=False,
    )

    src = (
        "INSERT INTO products (product_id, product_name, product_price, product_description) "
        "VALUES (1, 'A', 10.0, 'X')"
    )
    out = ts.apply_to_write_sql(src)
    norm = _flat(out)
    expect1 = _flat("INSERT INTO `products_core` (`product_id`,`product_name`,`product_price`) VALUES (1,'A',10.0)")
    expect2 = _flat("INSERT INTO `products_desc` (`product_id`,`product_description`) VALUES (1,'X')")
    assert expect1 in norm and expect2 in norm, f"insert split failed:\n{out}"


def test_write_update_split() -> None:
    ts = TableSplit(
        old_table='products',
        new_tables=['products_core', 'products_desc'],
        columnList={
            'products_core': ['product_id', 'product_name', 'product_price'],
            'products_desc': ['product_id', 'product_description'],
        },
        primary_keys_dict={'products_core': ['product_id'], 'products_desc': ['product_id']},
        new_view='view_products',
        is_retained=False,
    )

    src = "UPDATE products SET product_name='B', product_description='Y' WHERE product_id=1"
    out = ts.apply_to_write_sql(src)
    norm = _flat(out)
    expect1 = _flat("UPDATE `products_core` SET product_name='B' WHERE product_id=1")
    expect2 = _flat("UPDATE `products_desc` SET product_description='Y' WHERE product_id=1")
    assert expect1 in norm and expect2 in norm, f"update split failed:\n{out}"


def main() -> None:
    tests = [test_read_rewrite, test_write_insert_split, test_write_update_split]
    ok = 0
    for t in tests:
        t()
        ok += 1
        print(f"[OK] {t.__name__}")
    print(f"All {ok} tests passed.")


if __name__ == '__main__':
    main()
