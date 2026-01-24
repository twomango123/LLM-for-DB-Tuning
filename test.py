from rewrite.RedundantColumnAdd import RedundantColumnAdd  
from rewrite.TableSplit import TableSplit
from rewrite.TableJoin import TableJoin
from rewrite.RedundantColumnDrop import RedundantColumnDrop
from rewrite.HorizontalSplit import HorizontalSplit
from rewrite.HorizontalMerge import HorizontalMerge  
from rewrite.ColumnSplit import ColumnSplit  
from rewrite.ColumnRename import ColumnRename
from pathlib import Path
from typing import Iterator, Optional
import argparse
from DataBase.MySQLDriver import MySQLDriver
# python3 test5.py --write-back --apply-schema --db-host localhost --db-port 3306 --db-user root --db-password '123!@#200' --db-name customer_deliveries


def find_sql_files(root_dir: str) -> Iterator[Path]:
    root = Path(root_dir)
    if not root.exists():
        raise FileNotFoundError(root_dir)

    yield from root.rglob("*.sql")



def read_single_sql(sql_file: Path) -> str:
    sql = sql_file.read_text(encoding="utf-8").strip()

    if not sql:
        raise ValueError(f"Empty SQL file: {sql_file}")

    # 可选：去掉结尾分号
    if sql.endswith(";"):
        sql = sql[:-1].strip()

    return sql


def apply_op_to_sql_dir(sql_dir: str, op, write_back: bool = False, out_dir: Optional[str] = None):
    """
    遍历 sql_dir 下所有 .sql 文件，应用改写。
    - 当 write_back=True 时，直接覆盖原文件（仅当发生变化）。
    - 当 out_dir 指定时，将改写后的 SQL 写入 out_dir 下与原文件相对路径一致的位置，文件名保持不变（.sql）。
    二者同时指定时，优先写入 out_dir。
    """
    import re
    root = Path(sql_dir).resolve()
    out_root = Path(out_dir).resolve() if out_dir else None
    # 仅改写形如 queryN.sql 或 query_N.sql 的文件
    query_file_re = re.compile(r'^query_?\d+\.sql$', re.IGNORECASE)

    for sql_file in find_sql_files(sql_dir):
        # 若 out_dir 位于 sql_dir 内部，跳过对输出目录中文件的再次处理，避免
        # output_dir/rewritten/rewritten/... 的递归嵌套
        if out_root is not None:
            try:
                sql_path_resolved = Path(sql_file).resolve()
                if out_root in sql_path_resolved.parents:
                    continue
            except Exception:
                pass
        # 仅处理 queryN.sql 风格文件
        if not query_file_re.match(Path(sql_file).name):
            continue
        try:
            sql = read_single_sql(sql_file)
            new_sql = op.apply_to_sql(sql)

            print(f"[OK] {sql_file}")
            print("  OLD:", sql)
            print("  NEW:", new_sql)

            # 优先写入到 out_dir
            if out_root is not None:
                try:
                    rel = sql_file.resolve().relative_to(root)
                except Exception:
                    # 回退：仅使用文件名，避免路径计算失败
                    rel = Path(sql_file.name)
                dest = out_root / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text(new_sql + ";\n", encoding="utf-8")
                print(f"  -> WROTE: {dest}")
            elif write_back and new_sql != sql:
                sql_file.write_text(new_sql + ";\n", encoding="utf-8")
                print(f"  -> UPDATED IN-PLACE")

        except Exception as e:
            print(f"[ERROR] {sql_file}: {e}")


def apply_op_to_schema_mysql(op, host: str, port: int, user: str, password: str, database: str) -> bool:
    """在真实 MySQL 数据库上执行 schema 改写。"""
    cfg = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    db = MySQLDriver(cfg)
    if not db.connect():
        print("[ERROR] 数据库连接失败，已跳过 schema 改写。")
        return False
    try:
        ok = op.apply_to_schema(db=db)
        print(f"[SCHEMA] apply_to_schema result: {ok}")
        return bool(ok)
    finally:
        db.disconnect()

def main():
    parser = argparse.ArgumentParser(description="Rewrite SQLs and optionally apply schema changes to MySQL.")
    parser.add_argument("--sql-dir", default="output_dir", help="输入 SQL 根目录")
    parser.add_argument("--out-dir", default="output_dir/rewritten", help="输出目录：将改写后的 SQL 写入此目录下保持相对路径不变")
    parser.add_argument("--write-back", action="store_true", help="覆盖写回原 SQL 文件（当发生变化时）")

    # DB schema 变更相关参数
    parser.add_argument("--apply-schema", action="store_true", help="在真实 MySQL 数据库上执行 apply_to_schema")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", type=int, default=3306)
    parser.add_argument("--db-user", default="root")
    parser.add_argument("--db-password", default="")
    parser.add_argument("--db-name", default="tpcch")

    args = parser.parse_args()

    # 根据需要创建具体改写操作：Addresses_core + customer_addresses → Customer_Address_States
    actual_orders_cols = ['actual_order_id','order_status_code','regular_order_id','actual_order_date']
    order_deliveries_cols = ['location_code','actual_order_id','delivery_status_code','driver_employee_id','truck_id','delivery_date']
    op7 = RedundantColumnAdd('customer', 'c_state', 'orders_orderline_active', 'o_c_state', join_keys=[('c_w_id','o_w_id'),('c_d_id','o_d_id'),('c_id','o_c_id')])
    op9 = RedundantColumnAdd('customer', 'c_balance', 'orders_orderline_active', 'o_c_balance', join_keys=[('c_w_id','o_w_id'),('c_d_id','o_d_id'),('c_id','o_c_id')])
    op8 = HorizontalSplit('orders_orderline_active', [('orders_orderline_active_west', "o_w_id IN (1,2,3,4,5)"), ('orders_orderline_active_east', "o_w_id IN (6,7,8,9,10)")], is_retained=True)
    op = RedundantColumnAdd('customer', 'c_last', 'orders_orderline_active', 'o_c_last', join_keys=[('c_w_id','o_w_id'),('c_d_id','o_d_id'),('c_id','o_c_id')])
    apply_op_to_sql_dir(args.sql_dir, op, write_back=args.write_back, out_dir=args.out_dir)

    # 2) 如需：真实数据库执行 schema 变更
    if args.apply_schema:
        apply_op_to_schema_mysql(
            op,
            host=args.db_host,
            port=args.db_port,
            user=args.db_user,
            password=args.db_password,
            database=args.db_name,
        )


if __name__ == "__main__":
    main()
