from .base import SMO
# from ColumnMove import ColumnMove
# from ColumnCopy import ColumnCopy
# from ColumnRename import ColumnRename  
import pandas as pd
import os
import sqlglot
from sqlglot import expressions as exp
from log_info.log_info import get_logger

logger = get_logger()


class TableSplit(SMO):
    def __init__(self, old_table, new_tables, columnList, primary_keys_dict, new_view):
        self.old_table = old_table
        self.new_tables = new_tables  # 新表名列表
        self.columnList = columnList  # 每个新表对应的列名列表
        self.primary_keys_dict = primary_keys_dict  # 每个新表的主键列名
        self.new_view = new_view

    def apply_to_schema(self, db):
        
        print("=" * 60)
        print("🔍 开始表拆分操作")
        print(f"原表: {self.old_table}")
        print(f"新表: {self.new_tables}")
        print(f"主键字典: {self.primary_keys_dict}")
        print(f"业务列字典: {self.columnList}")
        
        # 收集所有主键列
        all_primary_keys = set()
        for table_name, pk_columns in self.primary_keys_dict.items():
            print(f"  {table_name} 的主键: {pk_columns}")
            all_primary_keys.update(pk_columns)
        
        print(f"所有主键列: {sorted(all_primary_keys)}")
        
        sql_statements = []
        
        # 1. 创建主键表
        pk_table_name = f"{self.old_table}_keys"
        old_table_quoted = f"`{self.old_table}`"
        pk_columns_str = ", ".join(sorted(all_primary_keys))
        
        sql1 = f"""CREATE TABLE `{pk_table_name}` AS
    SELECT DISTINCT {pk_columns_str}
    FROM {old_table_quoted};"""
        
        print(f"\n📝 SQL 1 - 创建主键表 '{pk_table_name}':")
        print(sql1)
        sql_statements.append(sql1)
        
        # 添加主键约束
        if all_primary_keys:
            sql2 = f"""ALTER TABLE `{pk_table_name}`
    ADD PRIMARY KEY ({pk_columns_str});"""
            
            print(f"\n📝 SQL 2 - 添加主键约束:")
            print(sql2)
            sql_statements.append(sql2)
        
        # 2. 创建业务表
        print(f"\n🔨 创建业务表:")
        for i, new_table in enumerate(self.new_tables):
            table_primary_keys = self.primary_keys_dict.get(new_table, [])
            business_columns = self.columnList.get(new_table, [])
            
            print(f"\n  表 {i+1}: {new_table}")
            print(f"    主键列: {table_primary_keys}")
            print(f"    业务列（原始）: {business_columns}")
            
            # 移除可能重复的主键列
            business_columns = [col for col in business_columns if col not in table_primary_keys]
            print(f"    业务列（去重后）: {business_columns}")
            
            # 合并列
            all_table_columns = table_primary_keys + business_columns
            all_columns_str = ", ".join(all_table_columns)
            
            sql = f"""CREATE TABLE `{new_table}` AS
    SELECT DISTINCT {all_columns_str}
    FROM {old_table_quoted};"""
            
            print(f"    SQL: CREATE TABLE {new_table}...")
            sql_statements.append(sql)
            
            # 添加主键约束
            if table_primary_keys:
                pk_str = ", ".join(table_primary_keys)
                pk_sql = f"""ALTER TABLE `{new_table}`
    ADD PRIMARY KEY ({pk_str});"""
                
                print(f"    主键约束: ALTER TABLE {new_table}...")
                sql_statements.append(pk_sql)
        
        # 3. 删除原表（可选）
        # sql_last = f"DROP TABLE IF EXISTS {old_table_quoted};"
        # print(f"\n🗑️ SQL 最后 - 删除原表:")
        # print(sql_last)
        # sql_statements.append(sql_last)
        
        print(f"\n📊 总共 {len(sql_statements)} 条 SQL 语句")
        
        # 执行 SQL
        results = []
        
        for i, sql in enumerate(sql_statements, 1):
            print(f"\n[{i}/{len(sql_statements)}] 执行SQL...")
            print(f"SQL: {sql[:100]}...")
            
            success = db.execute_statement(sql)
            
            if success:
                print("  ✅ 成功")
            else:
                print(f"  ❌ 失败")
                print("操作中止")
                return False, results
            
            results.append({
                'index': i,
                'sql': sql,
                'success': success
            })
        
        print(f"\n🎉 所有SQL执行成功")
        print(f"表拆分完成: {self.old_table} -> {self.new_tables}")
        return True, results
                
    def apply_to_data(self):
        pass

    def apply_to_sql(self):
        pass
    # ----------------------------------------------------------
    # SQL 改写操作
    # # ----------------------------------------------------------
    # 这个逻辑还不够完善，第一个需要考虑*代表查询表中所有列，第二点一旦遇到某个查询改写后包含多个原表拆分来的新表，需要增加新表连接操作才能确保查询到的数据和之前查询原表得到的一致，注意。
    def apply_to_readonly_sql(self, db, sql_path) :
        # 构建一个表 只保留old_table主属性列
        # 构建sql语句创建表
        # 将数据导入数据库表中
        # primary_key_table_name = f"{self.old_table}_keys"
        
        # 创建拆分后表的视图
        # view_name= self.create_logical_view(db, primary_key_table_name)

        # 逐个文件处理sql语句
        # 解析 替换from后表名为原表名self.old_table的表名为view_name
        output_sqls = self.process_sql_files(sql_path, self.new_view)
        
        # 将处理后的sql语句保存到文件中
        self._save_rewritten_sql(output_sqls, sql_path)

        return True
        
        
    def create_logical_view(self, db, primary_key_table_name):

        """
        创建逻辑视图，使用 self.columnList 获取业务表列信息
        """
        view_name = f"view_{self.old_table}"
        
        print(f"🔍 开始创建视图: {view_name}")
        print(f"主键表: {primary_key_table_name}")
        print(f"业务表: {self.new_tables}")
        print(f"列字典: {self.columnList}")
        
        # 1. 获取主键表的所有列（假设主键表只有主键列）
        primary_key_columns = set()
        for pk_list in self.primary_keys_dict.values():
            primary_key_columns.update(pk_list)
        
        print(f"所有主键列: {primary_key_columns}")
        
        # 2. 构建 SELECT 列列表
        select_columns = []
        used_columns = set()  # 跟踪已使用的列名
        
        # 2.1 首先添加主键表的列（用原始列名）
        for pk in sorted(primary_key_columns):
            col_expr = f"{primary_key_table_name}.{pk}"
            select_columns.append(f"{col_expr} AS {pk}")
            used_columns.add(pk)
        
        # 2.2 添加业务表的列（排除主键列，避免重复）
        for new_table in self.new_tables:
            if new_table in self.columnList:
                table_columns = self.columnList[new_table]
                print(f"处理表 '{new_table}' 的列: {table_columns}")
                
                for col in table_columns:
                    # 如果这个列是主键列，已经添加过了，跳过
                    if col in primary_key_columns:
                        continue
                    
                    # 如果列名已经用过（不同表可能有相同列名），添加表名前缀
                    if col in used_columns:
                        col_alias = f"{new_table}_{col}"
                    else:
                        col_alias = col
                    
                    col_expr = f"{new_table}.{col}"
                    select_columns.append(f"{col_expr} AS {col_alias}")
                    used_columns.add(col_alias)
        
        print(f"选择的列: {select_columns}")
        
        # 3. 构建 JOIN 条件
        join_clauses = []
        for new_table in self.new_tables:
            if new_table in self.primary_keys_dict:
                primary_keys = self.primary_keys_dict[new_table]
                join_conditions = []
                
                for pk in primary_keys:
                    join_conditions.append(f"{new_table}.{pk} = {primary_key_table_name}.{pk}")
                
                if join_conditions:
                    join_clauses.append(f"LEFT JOIN `{new_table}` ON {' AND '.join(join_conditions)}")
        
        # 4. 构建完整的 CREATE VIEW SQL
        if not select_columns:
            print("❌ 错误: 没有选择任何列")
            return None
        
        select_clause = ",\n    ".join(select_columns)
        join_clause = "\n".join(join_clauses)
        
        create_view_sql = f"""CREATE OR REPLACE VIEW `{view_name}` AS
    SELECT 
        {select_clause}
    FROM `{primary_key_table_name}`
    {join_clause};"""
        
        print(f"\n📝 生成的视图 SQL:")
        print(create_view_sql)
        
        # 5. 执行 SQL 创建视图
        try:
            success = db.execute_statement(create_view_sql)
            
            if success:
                print(f"✅ 视图 '{view_name}' 创建成功")
                return view_name
            else:
                print(f"❌ 视图 '{view_name}' 创建失败")
                return None
        except Exception as e:
            print(f"❌ 创建视图时出错: {e}")
            
        return view_name
    
    def process_sql_files(self, sql_path, view_name):
        output_sqls = {}
        
        if os.path.isdir(sql_path):
            # 处理文件夹中的所有SQL文件
            for filename in os.listdir(sql_path):
                if filename.endswith('.sql'):
                    file_path = os.path.join(sql_path, filename)
                    rewritten_sql = self._rewrite_sql_file(file_path, view_name)
                    output_sqls[filename] = rewritten_sql
        else:
            # 处理单个SQL文件
            filename = os.path.basename(sql_path)
            rewritten_sql = self._rewrite_sql_file(sql_path, view_name)
            output_sqls[filename] = rewritten_sql
        
        return output_sqls
    
    def _rewrite_sql_file(self, file_path, view_name):
        """重写单个SQL文件"""
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 分割SQL语句
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        rewritten_statements = []
        for sql in sql_statements:
            if sql.upper().startswith('SELECT'):
                # 替换FROM后的表名
                rewritten_sql = self._replace_table_name(sql, view_name)
                rewritten_statements.append(rewritten_sql)
            else:
                rewritten_statements.append(sql)
        
        return rewritten_statements
    
    def _replace_table_name(self, sql, view_name):
        """使用sqlglot替换表名"""
        print(f"\n处理表: {self.old_table} -> {view_name}")
        print(f"原始SQL: {sql[:100]}..." if len(sql) > 100 else f"原始SQL: {sql}")
        
        try:
            parsed = sqlglot.parse_one(sql, read='mysql')
            found_tables = []
            
            for table in parsed.find_all(sqlglot.exp.Table):
                full_name = table.name
                short_name = full_name.split('.')[-1]
                found_tables.append(full_name)
                
                if short_name == self.old_table:
                    new_name = f"{full_name.split('.')[0]}.{view_name}" if '.' in full_name else view_name
                    table.replace(sqlglot.exp.Table(this=sqlglot.exp.Identifier(this=new_name, quoted=False)))
                    print(f"✓ 替换: {full_name} -> {new_name}")
                    rewritten_sql = parsed.sql(dialect='mysql')
                    print(f"新SQL: {rewritten_sql[:100]}..." if len(rewritten_sql) > 100 else f"新SQL: {rewritten_sql}")
                    return rewritten_sql
            
            print(f"发现表: {found_tables}")
            print(f"未匹配到: {self.old_table}")
            
        except Exception as e:
            print(f"解析失败: {e}")
        
        # 手动替换
        old_pattern = f"tpcch.{self.old_table}"
        if old_pattern in sql:
            new_sql = sql.replace(old_pattern, f"tpcch.{view_name}")
            print(f"✓ 手动替换: {old_pattern} -> tpcch.{view_name}")
            print(f"新SQL: {new_sql[:100]}..." if len(new_sql) > 100 else f"新SQL: {new_sql}")
            return new_sql
        
        print(f"未找到表: {old_pattern}")
        return sql
    
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


