from __future__ import annotations
import argparse
import json
from dataclasses import dataclass
from typing import Optional, Dict, Any

from DataBase.MySQLDriver import MySQLDriver
from performance_eval.transform import type1, type2_remove_join, type2_add_join, type3_prune_filters
from performance_eval.sql_runner import explain_analyze_text, total_cost_from_plan_text


@dataclass
class ConnCfg:
    host: str
    port: int
    user: str
    password: str
    database: Optional[str]


def main():
    p = argparse.ArgumentParser(description='性能评估模块 - 端到端验证')
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=3306)
    p.add_argument('--user', required=True)
    p.add_argument('--password', required=True)
    p.add_argument('--database')

    sp = p.add_subparsers(dest='cmd', required=True)

    # 通用：给定 SQL 与 EXPLAIN 文件，跑估算与真实 EXPLAIN ANALYZE
    p1 = sp.add_parser('type1', help='接口1 - 行/列/选择率变化 验证')
    p1.add_argument('--plan', required=True)
    p1.add_argument('--sql', required=True)
    p1.add_argument('--target', required=True)
    p1.add_argument('--rows-factor', type=float, default=1.0)
    p1.add_argument('--cols-factor', type=float, default=1.0)
    p1.add_argument('--filter-factor', type=float, default=1.0)

    p2r = sp.add_parser('type2-remove', help='接口2 - 行为1 移除/替换 JOIN 验证')
    p2r.add_argument('--plan', required=True)
    p2r.add_argument('--sql-before', required=True)
    p2r.add_argument('--sql-after', required=True)
    p2r.add_argument('--old-tables', nargs='+', required=True)
    p2r.add_argument('--new-table', required=True)
    p2r.add_argument('--new-table-rows', type=float, required=True)
    p2r.add_argument('--cols-factor', type=float, default=1.0)

    p2a = sp.add_parser('type2-add', help='接口2 - 行为2 增加 JOIN 验证')
    p2a.add_argument('--plan', required=True)
    p2a.add_argument('--sql-before', required=True)
    p2a.add_argument('--sql-after', required=True)
    p2a.add_argument('--replace-target', required=True)
    p2a.add_argument('--replace-rows-factor', type=float, default=1.0)
    p2a.add_argument('--add-table', required=True)
    p2a.add_argument('--add-rows', type=float, required=True)
    p2a.add_argument('--join-type', choices=['hash', 'nested'], default='hash')
    p2a.add_argument('--join-sel', type=float, default=1e-6)
    p2a.add_argument('--cols-factor', type=float, default=1.0)
    p2a.add_argument('--filter-factor', type=float, default=1.0)

    p3 = sp.add_parser('type3', help='接口3 - 分片后过滤消除 验证')
    p3.add_argument('--plan', required=True)
    p3.add_argument('--sql-before', required=True)
    p3.add_argument('--sql-after', required=True)
    p3.add_argument('--patterns', nargs='+', required=True)
    p3.add_argument('--regex', action='store_true', default=False)
    p3.add_argument('--combine', choices=['product', 'min'], default='product')
    p3.add_argument('--cols-factor', type=float, default=1.0)

    args = p.parse_args()

    cfg = {
        'host': args.host,
        'port': args.port,
        'user': args.user,
        'password': args.password,
        'database': args.database,
    }
    db = MySQLDriver(cfg)
    if not db.connect():
        raise SystemExit('数据库连接失败')

    try:
        with open(args.plan, 'r') as f:
            plan_text = f.read()

        if args.cmd == 'type1':
            est = type1(plan_text,
                        target_table=args.target,
                        rows_factor=args.rows_factor,
                        cols_factor=args.cols_factor,
                        filter_factor=args.filter_factor)
            real = explain_analyze_text(db, args.sql)

        elif args.cmd == 'type2-remove':
            est = type2_remove_join(plan_text,
                                    old_tables=args.old_tables,
                                    new_table=args.new_table,
                                    new_table_rows=args.new_table_rows,
                                    cols_factor=args.cols_factor)
            real_before = explain_analyze_text(db, args.sql_before)
            real_after = explain_analyze_text(db, args.sql_after)
            real = json.dumps({
                'before': real_before,
                'after': real_after,
            }, ensure_ascii=False)

        elif args.cmd == 'type2-add':
            est = type2_add_join(plan_text,
                                 replace_target=args.replace_target,
                                 table_a=args.replace_target,
                                 replace_rows_factor=args.replace_rows_factor,
                                 add_table=args.add_table,
                                 add_rows=args.add_rows,
                                 join_type=args.join_type,
                                 join_sel=args.join_sel,
                                 cols_factor=args.cols_factor,
                                 filter_factor=args.filter_factor)
            real_before = explain_analyze_text(db, args.sql_before)
            real_after = explain_analyze_text(db, args.sql_after)
            real = json.dumps({
                'before': real_before,
                'after': real_after,
            }, ensure_ascii=False)

        elif args.cmd == 'type3':
            est = type3_prune_filters(plan_text,
                                      patterns=args.patterns,
                                      regex=args.regex,
                                      combine=args.combine,
                                      cols_factor=args.cols_factor)
            real_before = explain_analyze_text(db, args.sql_before)
            real_after = explain_analyze_text(db, args.sql_after)
            real = json.dumps({
                'before': real_before,
                'after': real_after,
            }, ensure_ascii=False)

        else:
            raise SystemExit('未知命令')

        # 汇总输出
        est_total = est.get('new_total_cost')
        est_orig = est.get('original_total_cost')
        out = {
            'estimation': est,
            'real_explain': real,
            'est_total_cost': est_total,
            'est_original_total_cost': est_orig,
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
    finally:
        db.disconnect()


if __name__ == '__main__':
    main()

