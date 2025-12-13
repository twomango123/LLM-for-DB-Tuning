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
        """
        策略1: FROM后包含任一旧表名就替换成新表，删除其他旧表
        """
        import re
        
        # 检查是否包含任一旧表名
        found_tables = []
        for table in self.old_tables:
            # 使用单词边界匹配表名
            pattern = re.compile(rf'\b{table}\b', re.IGNORECASE)
            if pattern.search(sql):
                found_tables.append(table)
        
        if not found_tables:
            return sql  # 不包含任何旧表名，不修改
        
        # 替换逻辑：将找到的第一个表名替换为新表名，删除其他表名
        new_sql = sql
        
        if len(found_tables) >= 1:
            # 替换第一个找到的表名
            first_table = found_tables[0]
            pattern = re.compile(rf'\b{first_table}\b', re.IGNORECASE)
            new_sql = pattern.sub(new_table_name, new_sql, count=1)
            
            # 删除其他旧表名（以逗号分隔的格式）
            for table in found_tables[1:]:
                # 匹配 ", table_name" 模式并删除
                pattern = re.compile(rf',\s*\b{table}\b', re.IGNORECASE)
                new_sql = pattern.sub('', new_sql, count=1)
        
        return new_sql

    def _replace_strategy2(self, sql, new_table_name):
        """策略2: FROM后同时包含两个旧表名才替换"""
        import re
        
        # 检查是否同时包含两个表
        contains_both = True
        for full_table_name in self.old_tables:
            if '.' in full_table_name:
                db_name, table_name = full_table_name.split('.')
            else:
                table_name = full_table_name
            
            # 检查是否包含表名（各种格式）
            patterns = [
                rf'\b{table_name}\b',
                rf'\b{full_table_name}\b',
                rf'\b{table_name}\s+(AS\s+)?\w+\b',
                rf'`{table_name}`'
            ]
            
            found = False
            for pattern_str in patterns:
                pattern = re.compile(pattern_str, re.IGNORECASE)
                if pattern.search(sql):
                    found = True
                    break
            
            if not found:
                contains_both = False
                break
        
        if not contains_both:
            return sql
        
        # 同时包含两个表，替换为新表
        new_sql = sql
        
        # 替换第一个表
        first_full_name = self.old_tables[0]
        if '.' in first_full_name:
            first_db, first_table = first_full_name.split('.')
        else:
            first_table = first_full_name
        
        # 多种替换尝试
        replacement_done = False
        
        # 1. 替换完整表名
        pattern_full = re.compile(rf'\b{first_full_name}\b', re.IGNORECASE)
        if pattern_full.search(new_sql):
            new_sql = pattern_full.sub(new_table_name, new_sql, count=1)
            replacement_done = True
        
        # 2. 替换简单表名
        if not replacement_done:
            pattern_simple = re.compile(rf'\b{first_table}\b', re.IGNORECASE)
            if pattern_simple.search(new_sql):
                new_sql = pattern_simple.sub(new_table_name, new_sql, count=1)
                replacement_done = True
        
        second_simple = self.old_tables[1].split('.')[-1]
        # 2. 删除第二个表名及其前面的逗号
        print(f"[DEBUG] 开始删除第二个表: {self.old_tables[1]}")
        
        # 方法1: 查找并删除 ", self.old_tables[1]" 模式
        pattern_comma_simple = re.compile(rf',\s*\b{re.escape(second_simple)}\b', re.IGNORECASE)
        if pattern_comma_simple.search(new_sql):
            new_sql = pattern_comma_simple.sub('', new_sql, count=1)
            print(f"[DEBUG] 删除逗号和简单表名: ,{second_simple}")
        
        # 方法2: 如果上面没删除成功，尝试完整表名
        elif '.' in self.old_tables[1]:
            pattern_comma_full = re.compile(rf',\s*\b{re.escape(self.old_tables[1])}\b', re.IGNORECASE)
            if pattern_comma_full.search(new_sql):
                new_sql = pattern_comma_full.sub('', new_sql, count=1)
                print(f"[DEBUG] 删除逗号和完整表名: ,{self.old_tables[1]}")
        
        # 方法3: 如果还没删除，直接查找表名并删除前面的逗号
        else:
            # 查找第二个表名的位置
            pattern_second = re.compile(rf'\b{re.escape(second_simple)}\b', re.IGNORECASE)
            match = pattern_second.search(new_sql)
            if match:
                start = match.start()
                # 向前查找逗号
                comma_pos = -1
                for i in range(start-1, max(-1, start-10), -1):
                    if new_sql[i] == ',':
                        comma_pos = i
                        break
                    elif not new_sql[i].isspace():
                        break
                
                if comma_pos != -1:
                    # 删除从逗号到表名结束的部分
                    new_sql = new_sql[:comma_pos] + new_sql[match.end():]
                    print(f"[DEBUG] 查找到并删除: ,{second_simple}")
                else:
                    # 没有逗号，直接删除表名
                    new_sql = new_sql[:start] + new_sql[match.end():]
                    print(f"[DEBUG] 直接删除表名: {second_simple}")
        return new_sql


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

    