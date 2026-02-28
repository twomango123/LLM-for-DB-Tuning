from __future__ import annotations
import argparse
import json
import os
from typing import Dict, List, Tuple

from DataBase.MySQLDriver import MySQLDriver
from rewrite.base import MySQLConstraintHelper
from rewrite.TableJoin import TableJoin
try:
    # 两个实现中，TableSplitCopy 提供了创建 <old>_keys 的实现，便于后续生成视图
    from rewrite.TableSplitCopy import TableSplit as TableSplitCopy
except Exception:
    TableSplitCopy = None
from rewrite.TableSplit import TableSplit as TableSplitView
from rewrite.RedundantColumnAdd import RedundantColumnAdd

from performance_eval.sql_runner import explain_analyze_text, total_cost_from_plan_text


def collect_table_columns(helper: MySQLConstraintHelper, table: str) -> List[str]:
    cons = helper.fetch_constraints(table)
    return [c['COLUMN_NAME'] for c in (cons.get('columns') or [])]


def read_sql_files(sql_dir: str) -> Dict[str, str]:
    sqls: Dict[str, str] = {}
    for fn in sorted(os.listdir(sql_dir)):
        if not fn.endswith('.sql'):
            continue
        p = os.path.join(sql_dir, fn)
        with open(p, 'r', encoding='utf-8') as f:
            sqls[fn] = f.read().strip().rstrip(';')
    return sqls


def write_sql_files(sql_dir: str, rewritten: Dict[str, str], suffix: str = 'after') -> None:
    out_dir = os.path.join(sql_dir, f'rewritten_{suffix}')
    os.makedirs(out_dir, exist_ok=True)
    for fn, sql in rewritten.items():
        with open(os.path.join(out_dir, fn), 'w', encoding='utf-8') as f:
            f.write(sql + ' \n')


def load_original_plan_map(plan_dir: str) -> Dict[str, str]:
    """将 queryN.sql -> part2_debug/explain/qN.txt 进行映射并读取文本。"""
    mapping: Dict[str, str] = {}
    for n in range(1, 100):
        qfn = f'query{n}.sql'
        pth = os.path.join(plan_dir, f'q{n}.txt')
        if os.path.exists(pth):
            with open(pth, 'r', encoding='utf-8') as f:
                mapping[qfn] = f.read()
    return mapping


def main():
    ap = argparse.ArgumentParser(description='按给定操作序列执行 EXPLAIN 前后对比')
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=3306)
    ap.add_argument('--user', required=True)
    ap.add_argument('--password', required=True)
    ap.add_argument('--database', required=True)
    ap.add_argument('--sql-dir', default='output_dir/sql')
    ap.add_argument('--plan-dir', default='part2_debug/explain')
    ap.add_argument('--dry-run', action='store_true', help='仅改写SQL并对比，不执行schema变更')

    args = ap.parse_args()

    # 读取原始 SQL 与原 explain
    sql_map = read_sql_files(args.sql_dir)
    orig_plan_map = load_original_plan_map(args.plan_dir)

    db = MySQLDriver({'host': args.host,
                      'port': args.port,
                      'user': args.user,
                      'password': args.password,
                      'database': args.database})
    if not db.connect():
        raise SystemExit('数据库连接失败')

    helper = MySQLConstraintHelper(db)

    # 记录“改写后 SQL”（逐步覆盖）
    after_sql: Dict[str, str] = dict(sql_map)

    # ---------- 操作1：TableJoin(customers, customer_addresses) -> customers_addresses_combined ----------
    try:
        cols_c = collect_table_columns(helper, 'customers')
        cols_ca = collect_table_columns(helper, 'customer_addresses')
        tj1 = TableJoin(old_tables=['customers', 'customer_addresses'],
                        new_table='customers_addresses_combined',
                        old_columns_list=[cols_c, cols_ca],
                        sign=1,  # 不保留旧表
                        join_key=[('customer_id', 'customer_id')])
        if not args.dry_run:
            tj1.apply_to_schema(db)
        # SQL 改写
        for k, sql in list(after_sql.items()):
            after_sql[k] = tj1.apply_to_sql(sql)
    except Exception as e:
        print(f'[WARN] TableJoin(customers, customer_addresses) 失败/跳过: {e}')

    # ---------- 操作2：VerticalSplit(products, True) -> high/low_frequency_products + 视图 ----------
    try:
        # 用 TableSplitCopy 创建 <old>_keys + 子表
        if TableSplitCopy is None:
            raise RuntimeError('TableSplitCopy 未可用')
        tsc = TableSplitCopy(
            old_table='products',
            new_tables=['high_frequency_products', 'low_frequency_products'],
            columnList=[
                ['product_id', 'product_name', 'product_price'],
                ['product_id', 'product_description']
            ],
            primary_keys_dict={
                'high_frequency_products': ['product_id'],
                'low_frequency_products': ['product_id']
            }
        )
        if not args.dry_run:
            tsc.apply_to_schema(db)
        # 创建逻辑视图并重写查询为视图（保持只读兼容）
        tsv = TableSplitView(
            old_table='products',
            new_tables=['high_frequency_products', 'low_frequency_products'],
            columnList={'high_frequency_products': ['product_id', 'product_name', 'product_price'],
                        'low_frequency_products': ['product_id', 'product_description']},
            primary_keys_dict={'high_frequency_products': ['product_id'], 'low_frequency_products': ['product_id']},
            new_view='view_products',
            is_retained=False
        )
        if not args.dry_run:
            # TableSplitCopy 创建了 `products_keys`
            tsv.create_logical_view(db, primary_key_table_name='products_keys')
        for k, sql in list(after_sql.items()):
            after_sql[k] = tsv.apply_to_sql(sql)
    except Exception as e:
        print(f'[WARN] VerticalSplit(products) 失败/跳过: {e}')

    # ---------- 操作3：TableJoin(actual_orders, regular_orders) -> combined_orders ----------
    try:
        cols_ao = collect_table_columns(helper, 'actual_orders')
        cols_ro = collect_table_columns(helper, 'regular_orders')
        tj2 = TableJoin(old_tables=['actual_orders', 'regular_orders'],
                        new_table='combined_orders',
                        old_columns_list=[cols_ao, cols_ro],
                        sign=1,
                        join_key=[('regular_order_id', 'regular_order_id')])
        if not args.dry_run:
            tj2.apply_to_schema(db)
        for k, sql in list(after_sql.items()):
            after_sql[k] = tj2.apply_to_sql(sql)
    except Exception as e:
        print(f'[WARN] TableJoin(actual_orders, regular_orders) 失败/跳过: {e}')

    # ---------- 操作4：RedundantColumnAdd(addresses.city -> customers_addresses_combined.customer_city) ----------
    try:
        rca1 = RedundantColumnAdd(
            source_table='addresses', source_column='city',
            target_table='customers_addresses_combined', new_column='customer_city',
            join_keys=[('address_id', 'address_id')]
        )
        if not args.dry_run:
            rca1.apply_to_schema(db)
        for k, sql in list(after_sql.items()):
            after_sql[k] = rca1.apply_to_sql(sql)
    except Exception as e:
        print(f'[WARN] RedundantColumnAdd(addresses.city -> customers_addresses_combined.customer_city) 失败/跳过: {e}')

    # ---------- 操作5：RedundantColumnAdd(products.product_price -> actual_order_products.redundant_product_price) ----------
    try:
        # 先以 products 为源（物理复制），再额外尝试对 view_products 的引用做 SQL 改写
        rca2 = RedundantColumnAdd(
            source_table='products', source_column='product_price',
            target_table='actual_order_products', new_column='redundant_product_price',
            join_keys=[('product_id', 'product_id')]
        )
        if not args.dry_run:
            rca2.apply_to_schema(db)
        for k, sql in list(after_sql.items()):
            sql2 = rca2.apply_to_sql(sql)
            # 若已改写为视图 view_products，再做一次替换
            if 'view_products' in sql2:
                rca2_view = RedundantColumnAdd(
                    source_table='view_products', source_column='product_price',
                    target_table='actual_order_products', new_column='redundant_product_price',
                    join_keys=[('product_id', 'product_id')]
                )
                sql2 = rca2_view.apply_to_sql(sql2)
            after_sql[k] = sql2
    except Exception as e:
        print(f'[WARN] RedundantColumnAdd(products.product_price -> actual_order_products.redundant_product_price) 失败/跳过: {e}')

    # 写出改写后的 SQL 供复查
    write_sql_files(args.sql_dir, after_sql, suffix='sequence')

    # ---------- 执行 EXPLAIN ANALYZE 对比 ----------
    results: Dict[str, Dict[str, str | float]] = {}
    for fn, sql in after_sql.items():
        before_text = orig_plan_map.get(fn)
        before_cost = total_cost_from_plan_text(before_text) if before_text else None
        after_text = explain_analyze_text(db, sql)
        after_cost = total_cost_from_plan_text(after_text) if after_text else None
        results[fn] = {
            'sql_after': sql,
            'before_explain': before_text or '',
            'after_explain': after_text or '',
            'before_total_cost': before_cost or 0.0,
            'after_total_cost': after_cost or 0.0,
            'delta': (after_cost or 0.0) - (before_cost or 0.0)
        }

    print(json.dumps(results, ensure_ascii=False, indent=2))

    db.disconnect()


if __name__ == '__main__':
    main()

