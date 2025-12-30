"""
- 测试脚本：
- 不连接真实数据库
- 详细信息在日志 app.log
- 操作：列重命名、属性拆分、表垂直拆分/合并、表水平拆分/合并、冗余列新增/删除。
"""

from ColumnRename import ColumnRename
from ColumnSplit import ColumnSplit
from TableSplit_copy import TableSplit
from TableMerge import TableMerge
from HorizontalSplit import HorizontalSplit
from HorizontalMerge import HorizontalMerge
from RedundantColumnAdd import RedundantColumnAdd
from RedundantColumnDrop import RedundantColumnDrop
import sys
import os
# 兼容脚本直接运行时的相对导入
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from log_info.log_info import get_logger


logger = get_logger()


class FakeDB:
    """替代MySQLDriver 打印 SQL 并返回空结果集"""

    def __init__(self):
        self.statements = []
        self.queries = []

    def execute_query(self, sql: str):
        self.queries.append(sql)
        # ColumnRename 中会读取当前数据库名
        if "SELECT DATABASE()" in sql.upper():
            return [{"db": "testdb"}]
        logger.debug(f"[查询] {sql.strip()}")
        return []

    def execute_statement(self, statement: str) -> bool:
        self.statements.append(statement)
        logger.debug(f"[执行] {statement.strip()}")
        return True


def title(s: str):
    logger.info("\n" + "=" * 16 + f" {s} " + "=" * 16)


def demo_column_rename():
    title("列重命名 ColumnRename")
    db = FakeDB()
    op = ColumnRename("orders", "old_col", "new_col")
    logger.info("- 执行模式变更（含外键处理）...")
    _ = op.apply_to_schema(db)
    sql = "SELECT old_col, amount FROM orders WHERE old_col > 10"
    logger.debug("- 改写前 SQL:\n" + sql)
    logger.debug("- 改写后 SQL:\n" + op.apply_to_sql(sql))


def demo_column_split():
    title("属性拆分 ColumnSplit")
    db = FakeDB()
    op = ColumnSplit("users", "email", ["email_user", "email_domain"], split_delimiter="@")
    logger.info("- 执行模式变更（CTAS+RENAME，处理主键/外键/默认值等）...")
    _ = op.apply_to_schema(db)
    sql = "SELECT email FROM users WHERE email LIKE '%@example.com'"
    logger.debug("- 改写前 SQL:\n" + sql)
    logger.debug("- 改写后 SQL:\n" + op.apply_to_sql(sql))


def demo_table_split():
    title("表垂直拆分 TableSplit")
    db = FakeDB()
    old = "student_courses"
    new_tables = ["students", "courses", "enrollments"]
    # 注意：TableSplit_copy 期望 columnList 为 {表名: 列数组} 的字典
    column_list = {
        "students": ["student_id", "student_name", "student_major"],
        "courses": ["course_id", "course_name", "credit"],
        "enrollments": ["student_id", "course_id", "grade", "semester"],
    }
    pk_dict = {
        "students": ["student_id"],
        "courses": ["course_id"],
        "enrollments": ["student_id", "course_id"],
    }
    # TableSplit_copy 的构造函数需要额外的 new_view 参数
    op = TableSplit(old, new_tables, column_list, pk_dict, new_view="view_" + old)
    logger.info("- 执行模式变更（创建主键表与业务表 + 迁移约束）...")
    _ = op.apply_to_schema(db)
    # TableSplit_copy 的 apply_to_sql 可能未实现或不接收参数，这里做兼容处理
    sql = "SELECT student_name, grade FROM student_courses"
    rewrite_fn = getattr(op, 'apply_to_sql', None)
    if callable(rewrite_fn):
        try:
            res = rewrite_fn(sql)
        except TypeError:
            try:
                res = rewrite_fn()
            except Exception:
                res = None
        if res is not None:
            logger.debug("- 改写前 SQL:\n" + sql)
            logger.debug("- 改写后 SQL:\n" + str(res))
        else:
            logger.info("- 当前使用 TableSplit_copy：apply_to_sql 不提供改写结果，跳过演示")
    else:
        logger.info("- 当前使用 TableSplit_copy：未提供 SQL 改写演示（apply_to_sql）")


def demo_table_merge():
    title("表垂直合并 TableMerge")
    db = FakeDB()
    t1_cols = ["id", "name", "age"]
    t2_cols = ["id", "address", "zipcode"]
    op = TableMerge(["t1", "t2"], "t_merged", [t1_cols, t2_cols], sign=1, join_key="id")
    logger.info("- 执行模式变更（全外连接模拟 + 迁移约束）...")
    _ = op.apply_to_schema(db)
    sql = "SELECT t1.id, t2.address FROM t1, t2 WHERE t1.id=t2.id"
    logger.debug("- 改写前 SQL:\n" + sql)
    logger.debug("- 改写后 SQL:\n" + op.apply_to_sql(sql))


def demo_horizontal_split():
    title("表水平拆分 HorizontalSplit")
    db = FakeDB()
    op = HorizontalSplit("orders", [("orders_2023", "year=2023"), ("orders_2024", "year=2024")])
    logger.info("- 执行模式变更（为每个分表复制约束与列属性）...")
    _ = op.apply_to_schema(db)
    # 用例1：唯一命中 orders_2023
    sql1 = "SELECT * FROM orders WHERE year=2023"
    logger.info("- 场景1：WHERE 命中单个子表 (year=2023) → 直接替换为 orders_2023")
    logger.debug("改写前:\n" + sql1)
    logger.debug("改写后:\n" + op.apply_to_sql(sql1))

    # 用例2：唯一命中 orders_2024（带别名）
    sql2 = "SELECT o.id FROM orders o WHERE o.year=2024"
    logger.info("- 场景2：WHERE 命中单个子表 (year=2024，含别名) → 直接替换为 orders_2024 并保留别名")
    logger.debug("改写前:\n" + sql2)
    logger.debug("改写后:\n" + op.apply_to_sql(sql2))

    # 用例3：无法唯一命中 → 使用 UNION ALL 子查询
    sql3 = "SELECT * FROM orders WHERE status='ok'"
    logger.info("- 场景3：WHERE 无法唯一命中任一子表 → 使用 UNION ALL 替换 FROM")
    logger.debug("改写前:\n" + sql3)
    logger.debug("改写后:\n" + op.apply_to_sql(sql3))


def demo_horizontal_merge():
    title("表水平合并 HorizontalMerge")
    db = FakeDB()
    op = HorizontalMerge(["orders_2023", "orders_2024"], "orders_all")
    logger.info("- 执行模式变更（从源表复制约束与列属性）...")
    _ = op.apply_to_schema(db)
    sql = "SELECT count(*) FROM orders_2023, other WHERE orders_2023.id = other.oid"
    logger.debug("- 改写前 SQL:\n" + sql)
    logger.debug("- 改写后 SQL:\n" + op.apply_to_sql(sql))


def demo_redundant_add_drop():
    title("冗余列新增/删除 RedundantColumnAdd/Drop")
    db = FakeDB()
    add = RedundantColumnAdd(
        source_table="customers",
        source_column="name",
        target_table="orders",
        new_column="customer_name_copy",
        join_keys=[("id", "customer_id")],
    )
    logger.info("- 执行冗余列新增（CTAS+RENAME + 复制约束与属性）...")
    _ = add.apply_to_schema(db)
    sql = "SELECT customers.name, orders.amount FROM customers, orders WHERE customers.id=orders.customer_id"
    logger.debug("- 新增冗余列后的查询改写（尽量移除已不需要的源表）\n改写前:\n" + sql)
    logger.debug("改写后:\n" + add.apply_to_sql(sql))

    drop = RedundantColumnDrop(
        target_table="orders",
        redundant_column="customer_name_copy",
        source_table="customers",
        source_column="name",
        join_keys=[("id", "customer_id")],
    )
    logger.info("- 执行冗余列删除（清理相关约束后 DROP COLUMN）...")
    _ = drop.apply_to_schema(db)
    sql2 = "SELECT orders.customer_name_copy FROM orders"
    logger.debug("- 删除冗余列后的查询改写（回退到源列）\n改写前:\n" + sql2)
    logger.debug("改写后:\n" + drop.apply_to_sql(sql2))


if __name__ == "__main__":
    logger.info("开始运行重写/模式变更功能测试（不连接真实数据库）...")
    demo_column_rename()
    demo_column_split()
    demo_table_split()
    demo_table_merge()
    demo_horizontal_split()
    demo_horizontal_merge()
    demo_redundant_add_drop()
    logger.info("测试完成。")
