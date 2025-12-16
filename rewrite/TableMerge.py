import pandas as pd
from io import StringIO
import os
from .base import SMO

class TableMerge(SMO):
    def __init__(self, old_tables, new_table, old_columns_list, sign):
        """
        old_tables: 旧表名列表 [table1, table2]
        new_table: 新表名
        old_columns_list: 两个旧表的所有列名 [[table1_columns], [table2_columns]]
        """
        self.old_tables = old_tables
        self.new_table = new_table
        self.old_columns_list = old_columns_list
        self.sign = sign #sign==1 不保留原表，==2保留原表建立物化视图
        

    def apply_to_schema(self, db):
        # 旧表不保留

        select_columns = []
        seen_columns = set()
        
        # 处理第一个表的列
        for col in self.old_columns_list[0]:
            if col == self.join_key and self.join_key:
                # 连接键只保留一次
                select_columns.append(f"COALESCE(t1.{col}, t2.{col}) AS {col}")
            else:
                select_columns.append(f"t1.{col}")
            seen_columns.add(col)
        
        # 处理第二个表的列，处理重复列名
        for col in self.old_columns_list[1]:
            if col == self.join_key and self.join_key:
                # 连接键已经在第一个表中处理过了，跳过
                continue
            elif col in seen_columns:
                # 重复列名，添加后缀
                new_col_name = f"{col}_2"
                select_columns.append(f"t2.{col} AS {new_col_name}")
            else:
                select_columns.append(f"t2.{col}")
        
        # 构建连接条件
        if self.join_key:
            join_condition = f"t1.{self.join_key} = t2.{self.join_key}"
        else:
            join_condition = "1=1"  # 笛卡尔积
        
        # 构建MySQL兼容的全外连接SQL语句
        union_sql = f"""
        CREATE TABLE {self.new_table} AS
        SELECT {', '.join(select_columns)}
        FROM {self.old_tables[0]} t1
        LEFT JOIN {self.old_tables[1]} t2
        ON {join_condition}
        
        UNION
        
        SELECT {', '.join(select_columns)}
        FROM {self.old_tables[0]} t1
        RIGHT JOIN {self.old_tables[1]} t2
        ON {join_condition}
        WHERE t1.{self.old_columns_list[0][0]} IS NULL
        """
        
        # 执行创建新表的SQL
        db.execute_statement(union_sql)
        
        # 删除旧表
        for old_table in self.old_tables:
            drop_sql = f"DROP TABLE {old_table}"
            db.execute_statement(drop_sql)
        

    def create_physical_view(self, db):
        # 旧表保留
        """
        创建两个旧表自然连接的物化视图（MySQL中就是创建一个新表）
        """
        # 找出两个表的公共列（自然连接的连接键）
        table1_cols = set(self.old_columns_list[0])
        table2_cols = set(self.old_columns_list[1])
        common_columns = list(table1_cols.intersection(table2_cols))
        
        if not common_columns:
            # 如果没有公共列，则使用笛卡尔积（所有行组合）
            print("警告：两个表没有公共列，将使用笛卡尔积连接")
            join_condition = "1=1"
            
            # 构建SELECT列表，处理重复列名
            select_columns = []
            
            # 第一个表的所有列
            for col in self.old_columns_list[0]:
                select_columns.append(f"t1.{col}")
            
            # 第二个表的所有列
            for col in self.old_columns_list[1]:
                if col in self.old_columns_list[0]:
                    # 列名重复，添加后缀
                    select_columns.append(f"t2.{col} AS {col}_2")
                else:
                    select_columns.append(f"t2.{col}")
        else:
            # 有公共列，使用自然连接
            # 构建连接条件（多个公共列时使用AND连接）
            join_conditions = []
            for col in common_columns:
                join_conditions.append(f"t1.{col} = t2.{col}")
            join_condition = " AND ".join(join_conditions)
            
            # 构建SELECT列表，公共列只出现一次
            select_columns = []
            
            # 第一个表的所有列
            for col in self.old_columns_list[0]:
                select_columns.append(f"t1.{col}")
            
            # 第二个表的非公共列
            for col in self.old_columns_list[1]:
                if col not in common_columns:
                    select_columns.append(f"t2.{col}")
        
        # 构建创建物化视图的SQL（在MySQL中就是创建表）
        create_sql = f"""
        CREATE TABLE {self.new_table} AS
        SELECT {', '.join(select_columns)}
        FROM {self.old_tables[0]} t1
        JOIN {self.old_tables[1]} t2
        ON {join_condition}
        """
        
        # 执行SQL创建物化视图
        db.execute_statement(create_sql)
        print(f"已创建物化视图（表）: {self.new_table}")

    def apply_to_data(self, data_dict):
        pass


    def apply_to_sql(self):
        pass

    def apply_to_readonly_sql(self, sql_path) :
        # 构建一个表 只保留old_table主属性列
        # 构建sql语句创建表
        # 将数据导入数据库表中
        # primary_key_table_name = f"{self.old_table}_keys"
        
        # 创建拆分后表的视图
        # new_table_name= self.create_logical_view(db, primary_key_table_name)

        # 逐个文件处理sql语句
        # 解析 替换from后表名为原表名self.old_table的表名为new_table_name
        output_sqls = self.process_sql_files(sql_path, self.new_table)
        
        # 将处理后的sql语句保存到文件中
        self._save_rewritten_sql(output_sqls, sql_path)

        return True
    
    def process_sql_files(self, sql_path, new_table_name):
        output_sqls = {}
        
        if os.path.isdir(sql_path):
            # 处理文件夹中的所有SQL文件
            for filename in os.listdir(sql_path):
                if filename.endswith('.sql'):
                    file_path = os.path.join(sql_path, filename)
                    rewritten_sql = self._rewrite_sql_file(file_path, new_table_name)
                    output_sqls[filename] = rewritten_sql
        else:
            # 处理单个SQL文件
            filename = os.path.basename(sql_path)
            rewritten_sql = self._rewrite_sql_file(sql_path, new_table_name)
            output_sqls[filename] = rewritten_sql
        
        return output_sqls
    
    def _rewrite_sql_file(self, file_path, new_table_name):
        """重写单个SQL文件
        
        假设所有SQL都是 FROM table1, table2 WHERE ... 的自然连接格式
        """
        sign = self.sign
        
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割SQL语句
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        rewritten_statements = []
        for sql in sql_statements:
            if sql.upper().startswith('SELECT'):
                # 根据sign选择不同的替换策略
                if sign == 1:
                    rewritten_sql = self._replace_strategy1(sql, new_table_name)
                elif sign == 2:
                    rewritten_sql = self._replace_strategy2(sql, new_table_name)
                else:
                    rewritten_sql = sql  # 默认不修改
                rewritten_statements.append(rewritten_sql)
            else:
                rewritten_statements.append(sql)
        
        return rewritten_statements

    def _replace_strategy1(self, sql, new_table_name):
        import re
        old_tables = set(self.old_tables)

        # 匹配每一个 from ...（直到 where / group / order / union / ) / 结尾）
        pattern = re.compile(
            r'(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)',
            re.IGNORECASE | re.DOTALL
        )

        def replace_from(match):
            prefix = match.group(1)
            tables_part = match.group(2)

            tables = [t.strip() for t in tables_part.split(',')]
            new_tables = []
            replaced = False

            for t in tables:
                base_table = t.split()[0]
                if base_table in old_tables:
                    if not replaced:
                        new_tables.append(t.replace(base_table, new_table_name, 1))
                        replaced = True
                    # 其余 old_table 表直接丢弃
                else:
                    new_tables.append(t)

            return prefix + ', '.join(new_tables)

        return pattern.sub(replace_from, sql)

    def _replace_strategy2(self, sql, new_table_name):
        import re
        old_tables = set(self.old_tables)

        pattern = re.compile(
            r'(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)',
            re.IGNORECASE | re.DOTALL
        )

        def replace_from(match):
            prefix = match.group(1)
            tables_part = match.group(2)

            tables = [t.strip() for t in tables_part.split(',')]
            base_tables = {t.split()[0] for t in tables}

            # 必须同时包含 old_tables 中的所有表
            if not old_tables.issubset(base_tables):
                return match.group(0)

            new_tables = []
            replaced = False

            for t in tables:
                base_table = t.split()[0]
                if base_table in old_tables:
                    if not replaced:
                        new_tables.append(new_table_name)
                        replaced = True
                    # 其余 old_table 表删除
                else:
                    new_tables.append(t)

            return prefix + ', '.join(new_tables)

        return pattern.sub(replace_from, sql)


    def _save_rewritten_sql(self, output_sqls, original_path):
        """保存重写后的SQL语句"""
        output_dir = os.path.join(os.path.dirname(original_path), "rewritten")
        os.makedirs(output_dir, exist_ok=True)
        
        for filename, sql_statements in output_sqls.items():
            output_path = os.path.join(output_dir, f"rewritten_{filename}")
            
            with open(output_path, 'w', encoding='utf-8') as f:
                for sql in sql_statements:
                    f.write(f"{sql};\n")
            
            print(f"已保存重写后的SQL到: {output_path}")



# 测试
if __name__ == "__main__":
    print("=== 简单测试 ===")
    
    # 创建实例
    merger = TableMerge(
        old_tables=["tpcch.order", "tpcch.orderline"],
        new_table="merged",
        old_columns_list=[[], []],
        sign=1
    )
    
    # 测试SQL
    test_cases = [
        "SELECT c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount) FROM tpcch.customer, tpcch.order,tpcch.orderline where c_id = o_c_id and c_w_id = o_w_id and c_d_id = o_d_id and ol_w_id = o_w_id group by o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt",
        "SELECT * FROM employees, departments",
        "SELECT * FROM departments"
    ]
    
    print("策略1:")
    merger.sign = 1
    for sql in test_cases:
        result = merger._replace_strategy1(sql, "merged_view")
        print(f"{sql} -> {result}")
    
    print("\n策略2:")
    merger.sign = 2
    for sql in test_cases:
        result = merger._replace_strategy2(sql, "merged_view")
        print(f"{sql} -> {result}")

    