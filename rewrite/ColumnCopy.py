from base import SMO
import pandas as pd


# 列复制
class ColumnCopy(SMO):
    """
    ColumnCopy: 在同一张表内复制列，不改变 SQL。
    新列统一命名为“副本”。
    
    例如：
        oldcolumn = "id"
    则：
        table["副本"] = table["id"]
    """

    def __init__(self, table, oldcolumn):
        self.table = table
        self.oldcolumn = oldcolumn
        self.newcolumn = f"{oldcolumn}_copy"
    
    def apply_to_schema(self, schema):
        pass

    def apply_to_sql(self, sql_ast):
        pass
    def apply_to_data(self, data_dict: dict):

        result = data_dict.copy()

        if self.table not in result:
            raise ValueError(f"[ColumnCopy] 表 {self.table} 不存在")

        df = result[self.table]

        if self.oldcolumn not in df.columns:
            raise ValueError(
                f"[ColumnCopy] 列 {self.oldcolumn} 在表 {self.table} 中不存在"
            )

        # 执行列复制，命名为“副本”
        df[self.newcolumn] = df[self.oldcolumn].values

        # 写回
        result[self.table] = df

        return result