# smo/smo_types.py

from .base import SMO
import re
from typing import List, Dict

def _detect_query_type(self) -> str:
        """检测 SQL 类型"""
        q = self.query.strip().upper()
        if q.startswith("SELECT"):
            return "SELECT"
        elif q.startswith("INSERT"):
            return "INSERT"
        elif q.startswith("UPDATE"):
            return "UPDATE"
        elif q.startswith("DELETE"):
            return "DELETE"
        else:
            return "UNKNOWN"

def get_tables(sql: str) -> List[str]:
    """提取sql涉及的表"""
    q = sql.query.upper()
    tables = []
    if sql.query_type == "SELECT":
        match = re.search(r"FROM\s+([\w,.\s]+)", sql.query, re.IGNORECASE)
        if match:
            tables = [t.strip() for t in match.group(1).split(",")]
    elif sql.query_type == "INSERT":
        match = re.search(r"INSERT\s+INTO\s+([\w.]+)", sql.query, re.IGNORECASE)
        if match:
            tables = [match.group(1).strip()]
    elif sql.query_type == "UPDATE":
        match = re.search(r"UPDATE\s+([\w.]+)", sql.query, re.IGNORECASE)
        if match:
            tables = [match.group(1).strip()]
    elif sql.query_type == "DELETE":
        match = re.search(r"DELETE\s+FROM\s+([\w.]+)", sql.query, re.IGNORECASE)
        if match:
            tables = [match.group(1).strip()]
    return tables

def get_columns(sql: str) -> List[str]:
    """提取涉及的列"""
    cols = []
    if sql.query_type == "SELECT":
        match = re.search(r"SELECT\s+(.*?)\s+FROM", sql.query, re.IGNORECASE | re.DOTALL)
        if match:
            cols = [c.strip() for c in match.group(1).split(",")]
    elif sql.query_type == "INSERT":
        match = re.search(r"\((.*?)\)\s*VALUES", sql.query, re.IGNORECASE | re.DOTALL)
        if match:
            cols = [c.strip() for c in match.group(1).split(",")]
    elif sql.query_type == "UPDATE":
        match = re.search(r"SET\s+(.*?)(\s+WHERE|\s*$)", sql.query, re.IGNORECASE | re.DOTALL)
        if match:
            assignments = match.group(1).split(",")
            for assign in assignments:
                col = assign.split("=")[0].strip()
                cols.append(col)
    elif sql.query_type == "DELETE":
        # DELETE 通常不涉及列，但 WHERE 里可能涉及
        cols = sql.get_where_columns()
    return cols

def get_where_columns(sql: str) -> List[str]:
    """提取 WHERE 子句中涉及的列"""
    cols = []
    match = re.search(r"WHERE\s+(.*?)(ORDER BY|GROUP BY|$)", sql.query, re.IGNORECASE | re.DOTALL)
    if match:
        where_clause = match.group(1)
        # 正则匹配列名 格式col = value
        potential_cols = re.findall(r"([\w.]+)\s*(=|>|<|>=|<=|!=|LIKE|IN)\s*", where_clause, re.IGNORECASE)
        cols = [c[0] for c in potential_cols]
    return cols


def rewrite_column_split(sql: str, table: str, old_col: str, new_cols: list, split_type="string", delimiter=":") -> str:
    """
    将涉及列拆分的 SQL 重写为使用新列，包括 SELECT、UPDATE、INSERT 和 WHERE 条件。
    
    :param sql: 原始 SQL 语句
    :param table: 表名
    :param old_col: 需要拆分的旧列
    :param new_cols: 拆分后的新列列表
    :param split_type: "string" 或 "number"
    :param delimiter: 字符串拆分的分隔符，默认 ":"
    :return: 重写后的 SQL
    """
    sql_lower = sql.lower()

    # -----------------
    # SELECT 类型
    # -----------------
    if sql_lower.startswith("select"):
        if split_type == "string":
            split_exprs = [
                f"SUBSTRING_INDEX(SUBSTRING_INDEX({old_col}, '{delimiter}', {i+1}), '{delimiter}', -1) AS {col}" 
                for i, col in enumerate(new_cols)
            ]
        else:
            split_exprs = [f"FLOOR({old_col} / {10**i}) % 10 AS {col}" for i, col in enumerate(new_cols)]
        sql = re.sub(rf"\b{old_col}\b", ", ".join(split_exprs), sql)

    # -----------------
    # UPDATE 类型
    # -----------------
    elif sql_lower.startswith("update"):
        set_exprs = ", ".join([f"{col} = ?" for col in new_cols])
        sql = re.sub(rf"{old_col}\s*=\s*\?", set_exprs, sql)

    # -----------------
    # INSERT 类型
    # -----------------
    elif sql_lower.startswith("insert"):
        sql = re.sub(rf"\b{old_col}\b", ", ".join(new_cols), sql)
        sql = re.sub(rf"\?", ", ".join(["?"]*len(new_cols)), sql, count=1)

    # -----------------
    # 处理 WHERE 子句中的 col OP value
    # -----------------
    # 简单匹配 col = 'value' 或 col = "value"
    pattern = rf"({old_col}\s*=\s*['\"]?([^'\"\s]+)['\"]?)"
    
    def where_replacer(match):
        value = match.group(2)
        if split_type == "string":
            values = value.split(delimiter)
            if len(values) != len(new_cols):
                # 拆分数量不匹配，警告或忽略
                values = ['']*len(new_cols)
        else:
            # 数字拆分示例
            values = [str((int(value)//(10**i)) % 10) for i in range(len(new_cols))]
        # 生成 AND 条件
        return " AND ".join([f"{col} = '{val}'" for col, val in zip(new_cols, values)])
    
    sql = re.sub(pattern, where_replacer, sql)

    return sql


def rewrite_column_merge(sql: str, table: str, old_cols: list, new_col: str, merge_type="string", delimiter=":") -> str:
    """
    将涉及列合并的 SQL 重写为使用新列，包括 SELECT、UPDATE、INSERT 和 WHERE 条件。

    :param sql: 原始 SQL
    :param table: 表名
    :param old_cols: 需要合并的旧列列表
    :param new_col: 合并后的新列名
    :param merge_type: "string" 或 "number"
    :param delimiter: 字符串合并的分隔符
    :return: 重写后的 SQL
    """
    sql_lower = sql.lower()

    # -----------------
    # SELECT 类型
    # -----------------
    if sql_lower.startswith("select"):
        if merge_type == "string":
            merge_expr = f"CONCAT_WS('{delimiter}', {', '.join(old_cols)}) AS {new_col}"
        else:
            # 简单数字合并示例，将数字按位置加权合并
            merge_expr = " + ".join([f"{col} * {10**i}" for i, col in enumerate(old_cols)]) + f" AS {new_col}"
        sql = re.sub(rf"\b{old_cols[0]}\b", merge_expr, sql)

    # -----------------
    # UPDATE 类型
    # -----------------
    elif sql_lower.startswith("update"):
        if merge_type == "string":
            merge_expr = f"CONCAT_WS('{delimiter}', {', '.join(old_cols)})"
        else:
            merge_expr = " + ".join([f"{col} * {10**i}" for i, col in enumerate(old_cols)])
        sql = re.sub(rf"{new_col}\s*=\s*\?", f"{new_col} = {merge_expr}", sql)

    # -----------------
    # INSERT 类型
    # -----------------
    elif sql_lower.startswith("insert"):
        sql = re.sub(rf"\b{', '.join(old_cols)}\b", new_col, sql)
        sql = re.sub(rf"\?", "?", sql)  # INSERT 的值可以统一用占位符或合并逻辑

    # -----------------
    # 处理 WHERE 子句中的多个旧列
    # -----------------
    # 简单匹配 old_col1 OP val1 AND old_col2 OP val2 ...
    pattern = " AND ".join([rf"({col}\s*=\s*['\"]?([^'\"\s]+)['\"]?)" for col in old_cols])
    
    def where_replacer(match):
        # 获取所有匹配的值
        values = [match.group(i*2+2) for i in range(len(old_cols))]
        if merge_type == "string":
            merged_val = delimiter.join(values)
        else:
            merged_val = sum([int(v)*(10**i) for i,v in enumerate(values)])
        return f"{new_col} = '{merged_val}'" if merge_type=="string" else f"{new_col} = {merged_val}"

    sql = re.sub(pattern, where_replacer, sql)

    return sql

def rewrite_table_split(sql: str, old_table: str, new_tables: list) -> str:
        """
        将 old_table 拆分为 new_tables 只替换表名。
        
        参数：
            old_table: 原表名
            new_tables: 新表名列表
        返回：
            新的 SQL 字符串
        """
        
        if old_table not in sql:
            return sql  # SQL中不包含原表，直接返回

        # 如果拆分到多个表，生成 UNION ALL
        if len(new_tables) > 1:
            rewritten_sqls = []
            for table in new_tables:
                temp_sql = re.sub(rf"\b{re.escape(old_table)}\b", table, sql, flags=re.IGNORECASE)
                rewritten_sqls.append(temp_sql)
            return " UNION ALL ".join(rewritten_sqls)

        # 单表拆分，直接替换表名
        sql = re.sub(rf"\b{re.escape(old_table)}\b", new_tables[0], sql, flags=re.IGNORECASE)
        return sql

# 列重命名
class ColumnRename(SMO):
    def __init__(self, table, old, new):
        self.table = table
        self.old = old
        self.new = new

    def apply_to_schema(self, schema):
        schema[self.table]["columns"][self.new] = schema[self.table]["columns"].pop(self.old)

    def apply_to_sql(self, sql):
        for col in get_columns(sql, ):
            if col.table == self.table and col.name == self.old:
                col.name = self.new
        return sql

    
    def apply_to_data(self, col):
        pass

# 列拆分  
class ColumnSplit(SMO):
    def __init__(self, table, old_column, new_columns, split_func):
        self.table = table
        self.old = old_column
        self.new_columns = new_columns
        self.split_func = split_func

    def apply_to_schema(self, schema):
        schema[self.table]["columns"].pop(self.old)
        for col in self.new_columns:
            schema[self.table]["columns"][col] = "TEXT"

    def apply_to_sql(self, sql):
        rewrite_column_split(sql, self.table, self.old, self.new_columns, split_type="string", delimiter=":")
        return sql

    
    
    def apply_to_data(self, row):
        if self.old in row:
            new_values = self.split_func(row[self.old])
            for col, val in zip(self.new_columns, new_values):
                row[col] = val
            del row[self.old]
        return row
    
# 列合并
class ColumnMerge(SMO):
    def __init__(self, table, columns, new_column, merge_func):
        self.table = table
        self.columns = columns
        self.new_column = new_column
        self.merge_func = merge_func

    def apply_to_schema(self, schema):
        for col in self.columns:
            schema[self.table]["columns"].pop(col)
        schema[self.table]["columns"][self.new_column] = "TEXT"

    def apply_to_sql(self, sql):
        rewrite_column_merge(sql, self.table, self.columns, self.new_column, merge_type="string", delimiter=":")  
        return sql

    def apply_to_data(self, row):
        values = [row[col] for col in self.columns if col in row]
        merged = self.merge_func(values)
        row[self.new_column] = merged
        for col in self.columns:
            row.pop(col, None)
        return row
    
# 表重命名
class TableRename(SMO):
    def __init__(self, old, new):
        self.old = old
        self.new = new

    def apply_to_schema(self, schema):
        schema[self.new] = schema.pop(self.old)

    def apply_to_sql(self, sql):
        pattern = rf"\b{re.escape(self.old)}\b"
        new_sql = re.sub(pattern, self.new, sql, flags=re.IGNORECASE)
        return new_sql

    def apply_to_data(self, row):
        return row  # 不改变 tuple
    
# 表拆分
class TableSplit(SMO):
    def __init__(self, old_table, new_tables, rules):
        self.old_table = old_table
        self.new_tables = new_tables   # ["person_basic","person_salary"]
        self.rules = rules             # {"person_basic":["name","age"], ...}

    def apply_to_schema(self, schema):
        for new, cols in self.rules.items():
            schema[new] = {"columns": {c: schema[self.old_table]["columns"][c] for c in cols}}

    def apply_to_sql(self, sql):
        new_sql = rewrite_table_split(sql, self.old_table, self.new_tables)
        return new_sql

    def apply_to_data(self, row):
        """
        输入: {"name":..., "age":..., "salary":...}
        输出: {"person_basic": {...}, "person_salary": {...}}
        """
        out = {}

        for new, cols in self.rules.items():
            out[new] = {c: row[c] for c in cols if c in row}

        return out
    
# 表连接
class TableMerge(SMO):
    def __init__(self, old_tables, new_table):
        self.old_tables = old_tables
        self.new_table = new_table

    def apply_to_schema(self, schema):
        new_cols = {}
        for t in self.old_tables:
            for col, typ in schema[t]["columns"].items():
                new_cols[col] = typ
        schema[self.new_table] = {"columns": new_cols}

    def apply_to_readonly_sql(self, sql):
        new_sql = rewrite_table_merge(sql, self.old_tables, self.new_table)
        return new_sql
    

    def apply_to_data(self, rows_for_each_table):
        # 输入: { "customer_info": {...}, "customer_finance": {...} }
        merged = {}
        for t in self.old_tables:
            merged.update(rows_for_each_table[t])
        return merged