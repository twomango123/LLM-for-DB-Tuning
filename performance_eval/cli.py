import argparse
import json
from .transform import type1, type2_remove_join, type2_add_join, type3_prune_filters


def main():
    p = argparse.ArgumentParser(description='Performance Evaluation Module')
    sub = p.add_subparsers(dest='cmd', required=True)

    p1 = sub.add_parser('type1', help='Adjust costs for rows/cols/selectivity changes')
    p1.add_argument('--plan', required=True)
    p1.add_argument('--target', dest='target_table')
    p1.add_argument('--target-tables', nargs='*')
    p1.add_argument('--rows-factor', type=float, default=1.0)
    p1.add_argument('--cols-factor', type=float, default=1.0)
    p1.add_argument('--filter-factor', type=float, default=1.0)
    p1.add_argument('--output', default='-')

    pr = sub.add_parser('type2', help='Behavior changes to joins')
    pr_sub = pr.add_subparsers(dest='subcmd', required=True)

    pr1 = pr_sub.add_parser('remove-join', help='Remove/replace join with a new table')
    pr1.add_argument('--plan', required=True)
    pr1.add_argument('--old-tables', nargs='+', required=True)
    pr1.add_argument('--new-table', required=True)
    pr1.add_argument('--new-table-rows', type=float, required=True)
    pr1.add_argument('--cols-factor', type=float, default=1.0)
    pr1.add_argument('--output', default='-')

    pr2 = pr_sub.add_parser('add-join', help='Replace target by A, then join A with B')
    pr2.add_argument('--plan', required=True)
    pr2.add_argument('--replace-target', required=True)
    pr2.add_argument('--table-a', dest='table_a')
    pr2.add_argument('--replace-rows-factor', type=float, default=1.0)
    pr2.add_argument('--add-table', required=True)
    pr2.add_argument('--add-rows', type=float, required=True)
    pr2.add_argument('--join-type', choices=['hash', 'nested'], default='hash')
    pr2.add_argument('--join-sel', type=float, default=1e-6)
    pr2.add_argument('--cols-factor', type=float, default=1.0)
    pr2.add_argument('--filter-factor', type=float, default=1.0)
    pr2.add_argument('--output', default='-')

    # Add type3 subcommand (top-level)
    p3 = sub.add_parser('type3', help='Prune filter costs for sharded/partitioned tables')
    p3.add_argument('--plan', required=True)
    p3.add_argument('--patterns', nargs='+', required=True, help='Filter text patterns or regex to match')
    p3.add_argument('--regex', action='store_true', default=False)
    p3.add_argument('--combine', choices=['product', 'min'], default='product')
    p3.add_argument('--cols-factor', type=float, default=1.0)
    p3.add_argument('--output', default='-')

    args = p.parse_args()

    with open(args.plan, 'r') as f:
        plan_text = f.read()

    if args.cmd == 'type1':
        res = type1(plan_text,
                    target_table=args.target_table,
                    target_tables=args.target_tables,
                    rows_factor=args.rows_factor,
                    cols_factor=args.cols_factor,
                    filter_factor=args.filter_factor)
    elif args.cmd == 'type2' and args.subcmd == 'remove-join':
        res = type2_remove_join(plan_text,
                                old_tables=args.old_tables,
                                new_table=args.new_table,
                                new_table_rows=args.new_table_rows,
                                cols_factor=args.cols_factor)
    elif args.cmd == 'type2' and args.subcmd == 'add-join':
        res = type2_add_join(plan_text,
                             replace_target=args.replace_target,
                             table_a=args.table_a,
                             replace_rows_factor=args.replace_rows_factor,
                             add_table=args.add_table,
                             add_rows=args.add_rows,
                             join_type=args.join_type,
                             join_sel=args.join_sel,
                             cols_factor=args.cols_factor,
                             filter_factor=args.filter_factor)
    elif args.cmd == 'type3':
        res = type3_prune_filters(plan_text,
                                  patterns=args.patterns,
                                  regex=args.regex,
                                  combine=args.combine,
                                  cols_factor=args.cols_factor)
    else:
        raise SystemExit('Unsupported command')

    if args.output == '-' or getattr(args, 'output', '-') == '-':
        print(json.dumps(res, ensure_ascii=False, indent=2))
    else:
        with open(args.output, 'w') as f:
            json.dump(res, f, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    main()
