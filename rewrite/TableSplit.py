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

        # 构建列→数据类型映射，用于对日期/时间列做零日期防护
        def _dtype_map(cons):
            mp = {}
            for cm in (cons.get('columns') or []):
                nm = cm.get('COLUMN_NAME')
                dt = (cm.get('DATA_TYPE') or '').lower()
                if nm:
                    mp[nm] = dt
            return mp

        dtype_map = _dtype_map(cons)

        def _safe_dt_expr(alias: str, col: str, dtype: str) -> str:
            # 与 TableJoin 中一致的零日期安全包装：
            # - DATE:      CAST(NULLIF(CONCAT(alias.`col`),'0000-00-00') AS DATE)
            # - DATETIME/TIMESTAMP: CAST(NULLIF(CONCAT(alias.`col`),'0000-00-00 00:00:00') AS DATETIME)
            q = f"{alias}.`{col}`"
            d = (dtype or '').lower()
            if d == 'date':
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00') AS DATE)"
            if d in ('datetime', 'timestamp'):
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00 00:00:00') AS DATETIME)"
            return q

        # 1) 创建每个业务表
        sql_statements = []
        logger.debug("创建业务表与列集合中...")
        for i, new_table in enumerate(self.new_tables):
            include = child_include_cols[new_table]
            logger.debug("子表%d: %s 列: %s", i + 1, new_table, include)
            # 为 CREATE TABLE ... SELECT 使用别名 t，以便安全引用
            select_exprs = []
            for col in include:
                dt = dtype_map.get(col, '')
                expr = _safe_dt_expr('t', col, dt)
                # 保持原列名
                select_exprs.append(f"{expr} AS `{col}`")
            cols_str = ", ".join(select_exprs)
            sql = f"""CREATE TABLE `{new_table}` AS
                SELECT DISTINCT {cols_str}
                FROM {old_table_quoted} t;"""
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

    def apply_to_write_sql(self, sql: str) -> str:
        """
        写入 SQL 改写（INSERT/UPDATE）：
        - is_retained=True（保留原表）：写入仍对原表生效，不改写（由视图只读使用）。
        - is_retained=False（实体垂直拆分）：
          * INSERT 到 old_table：将列按所属子表路由，拆分为多条 INSERT 到不同新表；
            - 若 INSERT ... SELECT，复用只读 apply_to_sql 改写 SELECT，并据列目标拆分；
            - 若 INSERT ... VALUES，多行批量时生成多条按表分组的 INSERT；
            - 仅在列清单清晰（明确列名）时执行；否则保守返回原 SQL。
          * UPDATE 到 old_table：按 SET 的列归属生成多条 UPDATE 针对各子表（仅当 WHERE 不引用跨子表列时）。
        注意：为了安全，本函数采取“保守路由”策略：无法明确归属即不改写。
        """
        if getattr(self, 'is_retained', False):
            return sql
        try:
            tree = sqlglot.parse_one(sql, read='mysql')
        except Exception:
            return sql

        old = self.old_table.split('.')[-1].lower()
        column_map = {t.lower(): set([c.lower() for c in (self.columnList.get(t) or [])]) for t in self.new_tables}

        def owner_table(col: str) -> str | None:
            c = col.lower().strip('`')
            for t, cols in column_map.items():
                if c in cols:
                    return t
            return None

        # INSERT
        if isinstance(tree, exp.Insert):
            schema = tree.this
            base = ''
            cols_nodes = []
            if isinstance(schema, exp.Schema):
                base = (schema.this.name or '').split('.')[-1].lower() if isinstance(schema.this, exp.Table) else ''
                cols_nodes = list(schema.expressions or [])
            elif isinstance(schema, exp.Table):
                base = (schema.name or '').split('.')[-1].lower()
            if base != old:
                return sql
            cols = [c.name for c in cols_nodes] if cols_nodes else []
            if not cols:
                return sql  # 需要明确列清单
            # 目标列分桶
            buckets: dict[str, list[int]] = {}
            for idx, c in enumerate(cols):
                t = owner_table(c)
                if not t:
                    return sql
                buckets.setdefault(t, []).append(idx)

            # 确保每个子表的主键列包含在其 INSERT 列中（共享键需要在每张子表写入）
            pkd = getattr(self, 'primary_keys_dict', {}) or {}
            for t, pk_cols in pkd.items():
                idxs = buckets.setdefault(t.lower(), [])
                for pk in (pk_cols or []):
                    if pk in cols:
                        i = cols.index(pk)
                        if i not in idxs:
                            idxs.append(i)
            # 统一按原 INSERT 列顺序排序，保证主键在前（若原列顺序在前）
            for t in list(buckets.keys()):
                buckets[t] = sorted(set(buckets[t]))

            # INSERT ... VALUES
            vals = tree.args.get('expression')
            out_stmts: list[str] = []
            if isinstance(vals, exp.Values):
                for t, idxs in buckets.items():
                    new_cols = [cols[i] for i in idxs]
                    rows = []
                    for row in vals.expressions:
                        items = [row.expressions[i].sql(dialect='mysql') for i in idxs]
                        rows.append('(' + ', '.join(items) + ')')
                    sql_stmt = f"INSERT INTO `{t}` (" + ', '.join(f"`{c}`" for c in new_cols) + ") VALUES " + ', '.join(rows) + ';'
                    out_stmts.append(sql_stmt)
                return '\n'.join(out_stmts)
            else:
                # INSERT ... SELECT
                sel = tree.args.get('expression')
                if not (isinstance(sel, exp.Select) or isinstance(sel, exp.Subquery)):
                    return sql
                # 按桶拆分 SELECT 的投影：保守做法，需要 SELECT 列与 INSERT 列一一对应
                sel_sql = sel.sql(dialect='mysql')
                sel_sql_rw = self.apply_to_sql(sel_sql)
                sel_new = sqlglot.parse_one(sel_sql_rw, read='mysql')
                if len(getattr(sel_new, 'expressions', [])) != len(cols):
                    return sql
                out = []
                for t, idxs in buckets.items():
                    new_cols = [cols[i] for i in idxs]
                    new_sel_exprs = [sel_new.expressions[i] for i in idxs]
                    sub = exp.Select(expressions=new_sel_exprs, from_=sel_new.args.get('from'), where=sel_new.args.get('where'), group=sel_new.args.get('group'), order=sel_new.args.get('order'))
                    stmt = exp.Insert(this=exp.Table(this=exp.Identifier(this=t))),
                    sql_stmt = f"INSERT INTO `{t}` (" + ', '.join(f"`{c}`" for c in new_cols) + ") " + sub.sql(dialect='mysql') + ';'
                    out.append(sql_stmt)
                return '\n'.join(out)
        
        # UPDATE
        if isinstance(tree, exp.Update):
            tbl = tree.this
            base = (tbl.name or '').split('.')[-1].lower() if isinstance(tbl, exp.Table) else ''
            if base != old:
                return sql
            sets = list(tree.args.get('expressions') or [])
            if not sets:
                return sql
            # 将赋值按列归属分桶
            bucket_sets: dict[str, list[exp.Expression]] = {}
            for s in sets:
                target = getattr(s, 'this', None)
                if not isinstance(target, exp.Column):
                    return sql
                t = owner_table(target.name)
                if not t:
                    return sql
                bucket_sets.setdefault(t, []).append(s)
            out = []
            for t, exprs in bucket_sets.items():
                u = exp.Update(this=exp.Table(this=exp.Identifier(this=t)), expressions=exprs, where=tree.args.get('where'))
                out.append(u.sql(dialect='mysql'))
            return '\n'.join(out)
        
        return sql
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

    # ---------- performance eval ----------
    def evaluate_on_plan(self, plan_text: str,
                         meta_path: str | None = 'output_dir/meta.json',
                         samples_path: str | None = 'response/samples/samples.json',
                         used_columns: dict | None = None) -> dict:
        """
        评估垂直拆分对计划的影响（增强版）：
        - 若查询引用 old_table：
          1) 判断所用列是否完全落入某个子表 → 落单则按该子表行宽缩放（行数不变），下游算子通过 type1 传播更新；
          2) 若涉及多个子表列：
             - is_retained=True：成本不变；
             - is_retained=False：在 1) 的宽度处理基础上，叠加一次“自然连接”代价，并据此更新连接输出行，借助 type1 对下游算子按行数变化进行缩放。
               连接代价使用 samples.json 的 join 中位数推导选择率 S，并用实际基数 L、R 缩放（hash 或 nested 简化模型）。
        - used_columns: 可选 {table_name: [cols...]}，若未提供，则用“最小宽度子表”近似。
        """
        try:
            from rewrite.cost_model import (
                load_meta, load_samples, width_from_columns, avg_row_width,
                join_selectivity_from_samples, CostModel
            )
            from performance_eval.rewrite_utils import (
                hash_join_cost_from_rows, nested_join_cost_from_rows
            )
            from performance_eval.plan import parse_plan, compute_total_cost
        except Exception:
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': 'performance_eval 不可用，跳过评估'
            }

        old = self.old_table
        nodes = parse_plan(plan_text)
        if not any(old in (n.tables or []) for n in nodes):
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': '计划未引用该表，跳过'
            }

        # 宽度近似：优先使用 used_columns 指示的列集合；否则回退为各子表全列
        old_cols = set()
        meta = load_meta(meta_path)
        for c, m in ((meta.get('tables') or {}).get(old, {}) or {}).get('columns', {}).items():
            old_cols.add(c)
        # 旧表估计行宽
        old_w = 0.0
        for c in old_cols:
            try:
                old_w += float(((meta.get('tables') or {}).get(old, {}).get('columns') or {}).get(c, {}).get('avg_length') or 0.0)
            except Exception:
                pass
        old_w = old_w or 1.0

        # 针对每个子表计算“本查询用到的列宽”
        child_ws: list[tuple[str, float]] = []
        for t in self.new_tables:
            cols = (used_columns or {}).get(t) or self.columnList.get(t, [])
            w = 0.0
            for c in cols:
                try:
                    w += float(((meta.get('tables') or {}).get(t, {}).get('columns') or {}).get(c, {}).get('avg_length') or 0.0)
                except Exception:
                    pass
            if w > 0:
                child_ws.append((t, w))
        if not child_ws:
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': '缺少列宽元数据，跳过'
            }

        # 检查是否“单子表覆盖所有所用列”
        cols_needed = set()
        for t in self.new_tables:
            cols_needed.update((used_columns or {}).get(t) or [])
        single_cover = None
        if cols_needed:
            for t, _ in child_ws:
                cols_t = set((used_columns or {}).get(t) or self.columnList.get(t, []) or [])
                if cols_needed.issubset(cols_t):
                    single_cover = t
                    break
        # 若没有显式列集合，采用最小宽度子表近似
        t_pick, w_child = (min(child_ws, key=lambda x: x[1]) if single_cover is None else
                           next((x for x in child_ws if x[0] == single_cover), child_ws[0]))
        cf = (w_child or 1.0) / max(old_w, 1e-9)
        cm = CostModel()
        res = cm.apply_type1(plan_text, target_table=old, rows_factor=1.0, cols_factor=cf, filter_factor=1.0)

        # 多子表列且不保留原表：叠加自然连接代价，并按选择率更新下游基数（经 type1 传播缩放）
        multi_tables = (single_cover is None and bool(cols_needed))
        if multi_tables and not getattr(self, 'is_retained', False):
            # 实际基数 L、R：用 meta 中各子表行数（如果没有子表视图，则近似用旧表基数）
            l_tbl, r_tbl = self.new_tables[0], self.new_tables[1] if len(self.new_tables) > 1 else (self.new_tables[0], None)
            L = float((meta.get('tables') or {}).get(l_tbl, {}).get('row_count') or (meta.get('tables') or {}).get(old, {}).get('row_count') or 0)
            R = float((meta.get('tables') or {}).get(r_tbl, {}).get('row_count') or (meta.get('tables') or {}).get(old, {}).get('row_count') or 0)
            samples = load_samples(samples_path) if samples_path else {}
            sel = join_selectivity_from_samples(samples)
            add_cost = 0.0
            if sel:
                S, sL, sR, sO = sel
                # 采用 HashJoin 模型估算新增连接代价
                add_cost = hash_join_cost_from_rows(L, R, sL, sR, sO)
            # 估算 JOIN 输出基数并将其传播到下游（行数缩放）
            base_rows = float((meta.get('tables') or {}).get(old, {}).get('row_count') or 0)
            out_rows = (L * R * (S if sel else 1e-6)) if (L and R) else base_rows
            rf = (out_rows / max(base_rows, 1e-9)) if base_rows else 1.0
            # 先按行数与行宽共同缩放一次，让 plan 树中各算子基数/成本随之更新
            res_rows = cm.apply_type1(res['new_plan_text'], target_table=old, rows_factor=rf, cols_factor=1.0, filter_factor=1.0)
            # 然后在新的总代价上叠加“自然连接”的新增代价
            before = res_rows.get('new_total_cost') or compute_total_cost(parse_plan(res_rows['new_plan_text']))
            res_rows['original_total_cost'] = res_rows.get('original_total_cost')
            res_rows['new_total_cost'] = before + add_cost
            res_rows['delta'] = (before + add_cost) - (res_rows.get('original_total_cost') or 0.0)
            # 附加“不能直接缩放得到的项”说明
            try:
                from rewrite.cost_model import parse_plan as _pp
                ns_terms = []
                for n in _pp(plan_text):
                    if 'join' in n.type:
                        ns_terms.append({'op': n.type, 'need': ['S(连接选择率)']})
                    if n.type in ('group_temp', 'group_agg'):
                        ns_terms.append({'op': n.type, 'need': ['G(分组数)']})
                    if n.type == 'index_scan':
                        ns_terms.append({'op': n.type, 'need': ['Sx(选择率)']})
                res_rows['non_scalable'] = ns_terms
            except Exception:
                pass
            res_rows['note'] = f"不保留原表：多子表列，已按选择率更新下游基数并叠加连接代价≈{add_cost:.3g}；宽度按 {t_pick} 缩放"
            return res_rows

        res['note'] = ('保留原表' if getattr(self, 'is_retained', False) else '不保留原表') + f'：按 {t_pick} 列宽缩放'
        # 不能直接通过缩放得到的项
        try:
            from rewrite.cost_model import parse_plan as _pp
            ns_terms = []
            for n in _pp(plan_text):
                if 'join' in n.type:
                    ns_terms.append({'op': n.type, 'need': ['S(连接选择率)']})
                if n.type in ('group_temp', 'group_agg'):
                    ns_terms.append({'op': n.type, 'need': ['G(分组数)']})
                if n.type == 'index_scan':
                    ns_terms.append({'op': n.type, 'need': ['Sx(选择率)']})
            if ns_terms:
                res['non_scalable'] = ns_terms
        except Exception:
            pass
        return res
