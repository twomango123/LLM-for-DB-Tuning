from sqlglot import parse_one, exp
import re
from base import SMO


# 列重命名
class ColumnRename(SMO):
    def __init__(self, table, old, new):
        self.table = table
        self.old = old
        self.new = new

    def apply_to_schema(self, schema):
        schema[self.table]["columns"][self.new] = schema[self.table]["columns"].pop(self.old)

    def apply_to_sql(self, sql):
        tree = parse_one(sql)

        for col in tree.find_all(exp.Column):
            if col.name == self.old:
                col.set("this", exp.to_identifier(self.new))

        print(tree.sql())

    
    def apply_to_data(self, data_dict):
        """
        将数据中的列名从旧列名改为新列名
        data_dict: 包含表数据的字典 {table_name: dataframe}
        """
        if self.table not in data_dict:
            raise ValueError(f"表 {self.table} 不在数据字典中")
        
        table_data = data_dict[self.table]
        
        if self.old not in table_data.columns:
            raise ValueError(f"列 {self.old} 不在表 {self.table} 中")
        
        # 重命名列
        table_data = table_data.rename(columns={self.old: self.new})
        data_dict[self.table] = table_data
        
        return data_dict