from DataBase.MySQLDriver import MySQLDriver as mysql
from rewrite import TableSplit_copy as ts

#  ^h^{   MySQLDriver  ^~  ^k
db_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "123!@#200",
    "database": "tpcch"
}

driver = mysql(db_config)
# driver.connect()

table_split = ts.TableSplit(
    old_table="orders",
    new_tables=["order_header", "order_detail"],
    primary_keys_dict={
        "order_header": ["o_w_id", "o_d_id", "o_id"],
        "order_detail": ["o_w_id", "o_d_id", "o_id"]
    },
    columnList={
        "order_header": [
            "o_id", "o_d_id", "o_w_id",
            "o_c_id", "o_entry_d", "o_carrier_id"
        ],
        "order_detail": [
            "o_id", "o_d_id", "o_w_id",
            "o_ol_cnt", "o_all_local"
        ]
    },
    new_view="order_view"
)
# success = table_split.apply_to_schema(driver)
# if success:
#     print("修改schema成功")
# else:
#     print("修改schema失败 ")
# success = table_split._create_primary_key_table(driver, "order_primary_key")
success = table_split.apply_to_readonly_sql(driver, "./DataBase/cleaned_sql/query_sql")
if success:
    print("重写成功")
else:
    print("重写失败 ")

# driver.disconnect()

