try:
    import pandas as pd  # 仅在数据级合并时使用
except Exception:
    pd = None
from io import StringIO
import os
try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  # pragma: no cover
    from base import SMO, MySQLConstraintHelper

class TableMerge(SMO):
    def __init__(self, old_tables, new_table, old_columns_list, sign, join_key=None):
        """
        old_tables: 旧表名列表 [table1, table2]
        new_table: 新表名
        old_columns_list: 两个旧表的所有列名 [[table1_columns], [table2_columns]]
        """
        self.old_tables = old_tables
        self.new_table = new_table
        self.old_columns_list = old_columns_list
        self.sign = sign #sign==1 不保留原表，==2保留原表建立物化视图
        self.join_key = join_key
        

    def apply_to_schema(self, db):
        """
        表垂直合并（旧表不保留）- MySQL 专用：
        - 先创建合并后的新表（模拟全外连接：LEFT JOIN UNION RIGHT JOIN过滤），
        - 使用约束助手迁移/重建约束：
          主键 = 两表主键与连接键的并集（去重复）；
          外键 = 两表全部出站外键映射到新表列；
          入站外键 = 所有引用旧表的子表外键重定向指向新表（在列名不变或可映射时）；
          唯一/检查/默认/自增 = 从旧表复制并在新表重建（列名冲突按映射处理）。
        - 最后删除旧表。
        """
        # 仅在“旧表不保留”时进行约束重建；否则创建物化视图并返回
        if getattr(self, 'sign', 1) != 1:
            # sign != 1 代表旧表保留，仅创建视图/物化表，不做约束迁移
            self.create_physical_view(db)
            return True
        t1, t2 = self.old_tables[0], self.old_tables[1]
        newt = self.new_table
        join_key = self.join_key

        helper = MySQLConstraintHelper(db)
        c1 = helper.fetch_constraints(t1)
        c2 = helper.fetch_constraints(t2)

        # 组装 SELECT 列与重命名映射
        select_columns = []
        seen_columns = set()

        # t1 列映射（基本保持同名；连接键使用 COALESCE 统一为 join_key 名字）
        rename_map_t1 = {}
        for col in self.old_columns_list[0]:
            if join_key and col == join_key:
                select_columns.append(f"COALESCE(t1.{col}, t2.{col}) AS `{col}`")
                rename_map_t1[col] = col
            else:
                select_columns.append(f"t1.{col} AS `{col}`")
                rename_map_t1[col] = col
            seen_columns.add(col)

        # t2 列映射（冲突列加 _2 后缀；连接键跳过，因为已由 t1 统一）
        rename_map_t2 = {}
        for col in self.old_columns_list[1]:
            if join_key and col == join_key:
                # 该列在新表中使用 t1 的同名列
                rename_map_t2[col] = join_key
                continue
            if col in seen_columns:
                new_col_name = f"{col}_2"
            else:
                new_col_name = col
            select_columns.append(f"t2.{col} AS `{new_col_name}`")
            rename_map_t2[col] = new_col_name

        # 连接条件
        if join_key:
            join_condition = f"t1.{join_key} = t2.{join_key}"
        else:
            join_condition = "1=1"

        # 创建新表（全外连接模拟）
        union_sql = f"""
        CREATE TABLE `{newt}` AS
        SELECT {', '.join(select_columns)}
        FROM `{t1}` t1
        LEFT JOIN `{t2}` t2
          ON {join_condition}
        UNION
        SELECT {', '.join(select_columns)}
        FROM `{t1}` t1
        RIGHT JOIN `{t2}` t2
          ON {join_condition}
        WHERE t1.{self.old_columns_list[0][0]} IS NULL
        """

        stmts: list[str] = []
        stmts.append('SET FOREIGN_KEY_CHECKS=0')
        stmts.append(union_sql)

        # 构建新表主键：两表主键 ∪ 连接键（去重）
        def _mapped_list(cols, rmap):
            res = []
            for c in cols or []:
                nc = rmap.get(c)
                if nc and nc not in res:
                    res.append(nc)
            return res

        pk1 = (c1.get('primary_key') or {}).get('columns') or []
        pk2 = (c2.get('primary_key') or {}).get('columns') or []
        new_pk_cols = []
        for col in _mapped_list(pk1, rename_map_t1):
            if col not in new_pk_cols:
                new_pk_cols.append(col)
        for col in _mapped_list(pk2, rename_map_t2):
            if col not in new_pk_cols:
                new_pk_cols.append(col)
        if join_key and join_key not in new_pk_cols:
            new_pk_cols.append(join_key)
        if new_pk_cols:
            stmts.append(
                f"ALTER TABLE `{newt}` ADD PRIMARY KEY (" + ", ".join(f"`{c}`" for c in new_pk_cols) + ")"
            )

        # 复制唯一约束/出站外键（来自 t1 和 t2）
        include_t1 = list(rename_map_t1.values())
        include_t2 = [v for (k, v) in rename_map_t2.items() if k != join_key]
        # t1
        stmts.extend(helper.build_add_constraints_for_table(newt, c1, include_t1, rename_map=rename_map_t1))
        # t2
        stmts.extend(helper.build_add_constraints_for_table(newt, c2, include_t2, rename_map=rename_map_t2))

        # 复制 CHECK 约束（简单表达式替换）
        import re as _re
        def rewrite_check(check_clause: str, rmap: dict) -> str:
            # 对 rmap 的 key 做词边界替换
            s = check_clause
            for k, v in sorted(rmap.items(), key=lambda x: -len(x[0])):
                if v is None:
                    continue
                s = _re.sub(rf"(?<!\.)\b{_re.escape(k)}\b", v, s)
            return s
        for src, cons, rmap in ((t1, c1, rename_map_t1), (t2, c2, rename_map_t2)):
            for ck in cons.get('checks', []) or []:
                clause = rewrite_check(ck['clause'], rmap)
                name = ck['name']
                # 防止重名，附加源表前缀
                name = f"{src}_{name}"
                stmts.append(
                    f"ALTER TABLE `{newt}` ADD CONSTRAINT `{name}` CHECK ({clause})"
                )

        # 复制默认值；自增：仅当该“新列”来自的源表中恰有一个带自增时才设置
        def _lit(v: str):
            if v is None or v == 'NULL':
                return 'NULL'
            try:
                float(v)
                return v
            except Exception:
                pass
            v = str(v).replace("'", "''")
            return f"'{v}'"

        # 聚合每个“新列”的属性
        from collections import defaultdict
        defaults_map = defaultdict(list)  # new_col -> [defaults...]
        ai_count = defaultdict(int)       # new_col -> number of sources with AI
        ctype_map = {}                    # new_col -> column type (prefer first)
        nullable_map = {}

        def fold_props(colmeta, rmap):
            for cm in colmeta or []:
                oldc = cm['COLUMN_NAME']
                newc = rmap.get(oldc)
                if not newc:
                    continue
                if newc not in ctype_map:
                    ctype_map[newc] = cm.get('COLUMN_TYPE') or 'varchar(255)'
                    nullable_map[newc] = cm.get('IS_NULLABLE', 'YES')
                defaults_map[newc].append(cm.get('COLUMN_DEFAULT'))
                if 'auto_increment' in (cm.get('EXTRA') or '').lower():
                    ai_count[newc] += 1

        fold_props(c1.get('columns'), rename_map_t1)
        fold_props(c2.get('columns'), rename_map_t2)

        # 应用默认与自增（自增仅在计数==1 时设置）
        for newc, lst in defaults_map.items():
            # 默认值：取第一个非 None 值（垂直合并时每个新列来源唯一，不会冲突）
            dv = next((x for x in lst if x is not None), None)
            if dv is not None:
                stmts.append(f"ALTER TABLE `{newt}` ALTER COLUMN `{newc}` SET DEFAULT {_lit(dv)}")
        for newc, cnt in ai_count.items():
            if cnt == 1:
                ctype = ctype_map.get(newc, 'varchar(255)')
                nullable = nullable_map.get(newc, 'YES')
                stmts.append(
                    f"ALTER TABLE `{newt}` MODIFY COLUMN `{newc}` {ctype} "
                    f"{'NOT NULL' if nullable=='NO' else 'NULL'} AUTO_INCREMENT"
                )

        # 入站外键重定向到新表（仅当所有引用列都能映射到新表列名时）
        def rebuild_inbound(cons, rmap):
            for fk in cons.get('foreign_keys_inbound', []) or []:
                child = fk['child_table']
                ref_cols_old = [ref for (_, ref) in fk['cols']]
                ref_cols_new = []
                ok = True
                for rc in ref_cols_old:
                    nc = rmap.get(rc)
                    if not nc:
                        ok = False
                        break
                    ref_cols_new.append(nc)
                if not ok:
                    continue
                # drop + add
                stmts.append(f"ALTER TABLE `{child}` DROP FOREIGN KEY `{fk['constraint_name']}`")
                child_cols = [c for (c, _) in fk['cols']]
                clause = (
                    f"ALTER TABLE `{child}` ADD CONSTRAINT `{fk['constraint_name']}` "
                    f"FOREIGN KEY (" + ", ".join(f'`{c}`' for c in child_cols) + ") "
                    f"REFERENCES `{newt}` (" + ", ".join(f'`{c}`' for c in ref_cols_new) + ")"
                )
                if fk.get('delete_rule'):
                    clause += f" ON DELETE {fk['delete_rule']}"
                if fk.get('update_rule'):
                    clause += f" ON UPDATE {fk['update_rule']}"
                stmts.append(clause)

        rebuild_inbound(c1, rename_map_t1)
        rebuild_inbound(c2, rename_map_t2)

        # 删除旧表
        for old_table in self.old_tables:
            stmts.append(f"DROP TABLE `{old_table}`")

        stmts.append('SET FOREIGN_KEY_CHECKS=1')

        # 执行
        for s in stmts:
            db.execute_statement(s)
        

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

    

    def apply_to_sql(self, sql: str) -> str:
        """
        将查询中 old_tables 的自然连接改写为对 new_table 的读取。
        仅处理只读 SELECT，且遵循“FROM t1, t2 ... WHERE ...”的常见格式。
        - 当 sign==1/2 使用与 apply_to_readonly_sql 内部相同的 from 替换策略。
        """
        return (self._replace_strategy1(sql, self.new_table) if self.sign == 1
                else self._replace_strategy2(sql, self.new_table))

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

    
