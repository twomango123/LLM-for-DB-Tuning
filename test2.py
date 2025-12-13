import re
from DataBase.MySQLDriver import MySQLDriver as mysql
from rewrite import TableMerge as tm

#  ^h^{   MySQLDriver  ^~  ^k
db_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123!@#200",
    "database": "tpcch"
}

driver = mysql(db_config)


old_tables = ["tpcch.order", "tpcch.orderline"]
new_table = "order_merged"
old_columns_list = [
    ["o_id", "o_d_id", "o_w_id"],  # order_header的列
    ["ol_id", "ol_d_id", "ol_w_id"]              # order_detail的列
]

# 创建TableMerge实例
table_merge = tm.TableMerge(
    old_tables=old_tables,
    new_table=new_table,
    old_columns_list=old_columns_list,
    sign=2
)

# 测试1: 创建物化视图（新表）
print("=== 测试1: 创建物化视图 ===")
try:
    # 假设TableMerge有create_physical_view方法
    if hasattr(table_merge, 'create_physical_view'):
        table_merge.create_physical_view(driver)
        print("✓ 成功创建物化视图")
    else:
        print("✗ TableMerge没有create_physical_view方法")
except Exception as e:
    print(f"✗ 创建物化视图失败: {e}")

# 测试2: 重写SQL文件
print("\n=== 测试2: 重写SQL文件 ===")

success = table_merge.apply_to_readonly_sql("./DataBase/cleaned_sql/query_sql")

if success:
    print("重写成功")
else:
    print("重写失败 ")