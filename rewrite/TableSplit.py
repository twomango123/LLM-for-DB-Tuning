import pandas as pd
import re
from base import SMO
from ColumnMove import ColumnMove



class TableSplit(SMO):
    def __init__(self, old_table, new_tables, columnList):
        """
        表拆分操作
        old_table: 原表名
        new_tables: 新表名列表 [table1, table2]  如果多个表应该逐个拆分 多次调用
        columnList: 要迁移的列 列表 [[table1_columns], [table2_columns]]
        """
        self.old_table = old_table
        self.new_tables = new_tables
        self.columnList = columnList
        
        # 创建 列迁移 操作列表
        self.column_moves = []
        for i, columns in enumerate(columnList):
            for column in columns:
                self.column_moves.append(ColumnMove(
                    oldtable=old_table,
                    oldcolumn=column,
                    newtable=new_tables[i],
                    newcolumn=column
                ))

    def apply_to_sql(self, sql):
        """
        逐个调用列迁移的apply_to_sql
        """
        rewritten_sql = sql
        for column_move in self.column_moves:
            rewritten_sql = column_move.apply_to_sql(rewritten_sql)
        return rewritten_sql

    def apply_to_data(self, data_dict):
        """
        逐个调用列迁移的apply_to_data
        """
        result_data = data_dict.copy()
        for column_move in self.column_moves:
            result_data = column_move.apply_to_data(result_data)
        return result_data