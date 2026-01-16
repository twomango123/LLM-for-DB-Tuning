try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  
    from base import SMO, MySQLConstraintHelper

try:
    import pandas as pd
except Exception:
    pd = None
import os
import re
import sqlglot
from sqlglot import expressions as exp
try:
    from log_info.log_info import get_logger
except Exception:
    import sys as _sys, os as _os
    _sys.path.append(_os.path.dirname(_os.path.dirname(__file__)))
    from log_info.log_info import get_logger

logger = get_logger()


class TableSplit(SMO):
    def __init__(self, old_table, new_tables, columnList, primary_keys_dict, new_view, is_retained=False):
        self.old_table = old_table
        self.new_tables = new_tables  # 新表名列表
        self.columnList = columnList  # 每个新表对应的列名列表
        self.primary_keys_dict = primary_keys_dict  # 每个新表的主键列名
        self.new_view = new_view
        self.is_retained = is_retained #是否保留原表

    def apply_to_schema(self, db):
        logger.info("开始表拆分（不保留原表）: %s -> %s", self.old_table, ", ".join(self.new_tables))

        old_table_quoted = f"`{self.old_table}`"
        helper = MySQLConstraintHelper(db)
        cons = helper.fetch_constraints(self.old_table)
        orig_pk = (cons.get('primary_key') or {}).get('columns') or []
        if not orig_pk:
            logger.warning("原表未检测到主键，建议设置主键后再执行拆分")

        # 规则检查：外键不得被拆在不同子表
        # 这里的策略：若某个外键引用列集合无法完全落在某个子表的列集合中，直接报错
        # 同时，后续仅在“第一个满足条件的子表”上重建该外键，避免一条外键被复制到多个子表

        # 构造每个子表应包含的列集合（原表主键 + 业务列）
        child_include_cols = {}
        for t in self.new_tables:
            bcols = list(dict.fromkeys(self.columnList.get(t, [])))
            # 去重并前置原表主键
            include = list(dict.fromkeys(orig_pk + [c for c in bcols if c not in orig_pk]))
            child_include_cols[t] = include

        # 1) 创建每个业务表
        sql_statements = []
        logger.debug("创建业务表与列集合中...")
        for i, new_table in enumerate(self.new_tables):
            include = child_include_cols[new_table]
            logger.debug("子表%d: %s 列: %s", i + 1, new_table, include)
            cols_str = ", ".join(include)
            sql = f"""CREATE TABLE `{new_table}` AS
                SELECT DISTINCT {cols_str}
                FROM {old_table_quoted};"""
            sql_statements.append(sql)

        # 2) 为每个子表重建主键（使用原表主键）+ 其它约束（唯一、检查、出站外键不在这里处理）
        for new_table in self.new_tables:
            if orig_pk:
                pk_str = ", ".join(orig_pk)
                sql_statements.append(f"ALTER TABLE `{new_table}` ADD PRIMARY KEY ({pk_str});")

        # 3) 唯一/检查约束重建（只要其列全集在子表内）
        for new_table in self.new_tables:
            include = child_include_cols[new_table]
            add_stmts = helper.build_add_constraints_for_table(new_table, cons, include, rename_map=None)
            # 过滤：已手动添加了主键；外键稍后单独处理；保留 UNIQUE/CHECK
            for s in add_stmts:
                su = s.upper()
                if ' ADD PRIMARY KEY ' in su:
                    continue
                if ' FOREIGN KEY ' in su:
                    continue
                sql_statements.append(s)

        # 4) 外键约束定位并在单一子表重建
        fk_assigned = set()
        for fk in cons.get('foreign_keys_outbound', []) or []:
            child_cols = [c for (c, _, _) in fk['cols']]
            # 找能完全包含该外键列集合的子表
            candidates = [t for t in self.new_tables if set(child_cols).issubset(set(child_include_cols[t]))]
            if not candidates:
                raise ValueError(f"外键约束 {fk['constraint_name']} 的列 {child_cols} 无法完整落在某个子表中，违反外键不得垂直拆分规则")
            chosen = candidates[0]
            fk_assigned.add((fk['constraint_name'], chosen))
            ref_table = fk['cols'][0][1]
            ref_cols = [rc for (_, _, rc) in fk['cols']]
            cols_sql = ", ".join(f"`{c}`" for c in child_cols)
            ref_sql = ", ".join(f"`{c}`" for c in ref_cols)
            cname = f"{fk['constraint_name']}_{chosen}"
            clause = (
                f"ALTER TABLE `{chosen}` ADD CONSTRAINT `{cname}` FOREIGN KEY ({cols_sql}) "
                f"REFERENCES `{ref_table}` ({ref_sql})"
            )
            if fk.get('delete_rule'):
                clause += f" ON DELETE {fk['delete_rule']}"
            if fk.get('update_rule'):
                clause += f" ON UPDATE {fk['update_rule']}"
            sql_statements.append(clause)

        # 5) 列属性：默认值 / 自增
        colmeta = cons.get('columns') or []
        def _lit(v: str):
            if v is None or v == 'NULL':
                return 'NULL'
            try:
                float(v)
                return str(v)
            except Exception:
                pass
            return "'" + str(v).replace("'", "''") + "'"
        for new_table in self.new_tables:
            include = set(child_include_cols[new_table])
            for cm in colmeta:
                col = cm['COLUMN_NAME']
                if col not in include:
                    continue
                default = cm.get('COLUMN_DEFAULT')
                extra = (cm.get('EXTRA') or '').lower()
                ctype = cm.get('COLUMN_TYPE') or 'varchar(255)'
                nullable = cm.get('IS_NULLABLE', 'YES')
                if default is not None:
                    sql_statements.append(
                        f"ALTER TABLE `{new_table}` ALTER COLUMN `{col}` SET DEFAULT {_lit(default)}"
                    )
                if 'auto_increment' in extra:
                    sql_statements.append(
                        f"ALTER TABLE `{new_table}` MODIFY COLUMN `{col}` {ctype} "
                        f"{'NOT NULL' if nullable=='NO' else 'NULL'} AUTO_INCREMENT"
                    )

        logger.info("将执行 %d 条SQL", len(sql_statements))
        results = []
        for i, sql in enumerate(sql_statements, 1):
            logger.debug("[%d/%d] SQL: %s", i, len(sql_statements), sql)
            success = db.execute_statement(sql)
            if success:
                logger.debug("执行成功")
            else:
                logger.error("执行失败，已中止")
                return False, results
            results.append({'index': i,'sql': sql,'success': success})
        logger.info("表拆分完成: %s -> %s", self.old_table, ", ".join(self.new_tables))
        return True, results
                
    def apply_to_data(self):
        pass

    def apply_to_sql(self, sql: str) -> str:
        """
        只读 SQL 改写：
        - is_retained=True：保持原 SQL 不变；
        - is_retained=False：将 FROM 中的 old_table 替换为拆分后构建的视图 self.new_view，保留原有别名。
        """
        if getattr(self, 'is_retained', False):
            return sql

        view_name = self.new_view or f"view_{self.old_table}"
        try:
            parsed = sqlglot.parse_one(sql, read='mysql')
            changed = False
            for table in list(parsed.find_all(exp.Table)):
                full_name = table.name
                short_name = full_name.split('.')[-1] if full_name else ''
                if short_name == self.old_table:
                    alias = table.args.get('alias')
                    new_tbl = exp.Table(this=exp.Identifier(this=view_name, quoted=False))
                    new_node = exp.Alias(this=new_tbl, alias=alias) if alias is not None else new_tbl
                    table.replace(new_node)
                    changed = True
            return parsed.sql(dialect='mysql') if changed else sql
        except Exception:
            # 兜底：仅替换无 schema 前缀的表名，以及常见 schema.old_table 形式
            # 优先替换 schema.old_table → schema.view_name
            # 若不存在 schema 前缀，则替换裸表名
            sql2 = re.sub(rf"\b([a-zA-Z0-9_]+)\.{re.escape(self.old_table)}\b",
                          rf"\1.{view_name}", sql)
            if sql2 == sql:
                sql2 = re.sub(rf"\b{re.escape(self.old_table)}\b", view_name, sql)
            return sql2
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
