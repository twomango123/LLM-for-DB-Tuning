# smo/smo_types.py

from base import SMO
import re
from typing import List, Dict
from sqlglot import parse_one, exp
import re


# 列复制
class ColumnCopy(SMO):
    def __init__(self, table, column):

        self.table = table

    def apply_to_sql(self, sql_ast):
        pass
    def apply_to_data(self, col):
        pass
    
        



# 表拆分
class TableSplit(SMO):
    def __init__(self, old_table, new_tables, columnList):
        self.old_table = old_table
        self.new_tables = new_tables  
                

    def apply_to_schema(self):
        pass

    def apply_to_sql(self, sql):
        # 多次调用 列迁移 Column Move
        # 原表拆分成两个新表相当于原表rename，先调用迁移再调用rename
        return sql

    def apply_to_data(self):
        # 逻辑同sql
        pass
        

