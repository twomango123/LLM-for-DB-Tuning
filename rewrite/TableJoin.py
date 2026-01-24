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

class TableJoin(SMO):
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

        t1_cols_set = set(self.old_columns_list[0])
        t2_cols_set = set(self.old_columns_list[1])

        # 针对 MySQL 严格模式下的零日期（'0000-00-00' / '0000-00-00 00:00:00'）做安全包装，
        # 避免 CTAS 时因 NO_ZERO_DATE / NO_ZERO_IN_DATE 报错。
        def _dtype_map(cons):
            mp = {}
            for cm in (cons.get('columns') or []):
                nm = cm.get('COLUMN_NAME')
                dt = (cm.get('DATA_TYPE') or '').lower()
                if nm:
                    mp[nm] = dt
            return mp

        dtype_t1 = _dtype_map(c1)
        dtype_t2 = _dtype_map(c2)

        def _safe_dt_expr(alias: str, col: str, dtype: str) -> str:
            """返回一个在严格模式下也安全的日期/时间表达式。
            - 对于 DATE：CAST(NULLIF(CONCAT(alias.`col`),'0000-00-00') AS DATE)
            - 对于 DATETIME/TIMESTAMP：CAST(NULLIF(CONCAT(alias.`col`),'0000-00-00 00:00:00') AS DATETIME)
            - 其他类型：alias.`col`
            解释：先用 CONCAT 强制转成字符串再与零日期比较，避免直接将无效零日期字面量转为 datetime 类型引发错误；
            NULLIF 零日期 → NULL；随后 CAST 回目标类型。
            """
            d = (dtype or '').lower()
            q = f"{alias}.`{col}`"
            if d == 'date':
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00') AS DATE)"
            if d in ('datetime', 'timestamp'):
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00 00:00:00') AS DATETIME)"
            return q

        # 解析 join_key → join_pairs, unify_cols
        # - join_pairs: [(t1_col, t2_col), ...] 用于 ON 条件
        # - unify_cols: {col} 当且仅当 t1_col == t2_col 时，选择使用 COALESCE 合并为单列
        join_pairs = []
        unify_cols = set()
        if join_key:
            if isinstance(join_key, (list, tuple)):
                # (a,b) 或 [(a,b), ...]
                if len(join_key) == 2 and all(not isinstance(x, (list, tuple)) for x in join_key):
                    a, b = join_key  # 单对
                    if a in t1_cols_set and b in t2_cols_set:
                        join_pairs = [(a, b)]
                        if a == b:
                            unify_cols.add(a)
                else:
                    for pair in join_key:
                        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                            continue
                        a, b = pair
                        if a in t1_cols_set and b in t2_cols_set:
                            join_pairs.append((a, b))
                            if a == b:
                                unify_cols.add(a)
            elif isinstance(join_key, str):
                if join_key in t1_cols_set and join_key in t2_cols_set:
                    join_pairs = [(join_key, join_key)]
                    unify_cols.add(join_key)

        # t1 列映射（基本保持同名；unify 列使用 COALESCE(t1.c, t2.c) 统一），
        # 同时对日期/时间列应用零日期安全包装。
        rename_map_t1 = {}
        for col in self.old_columns_list[0]:
            if col in unify_cols:
                dt1 = dtype_t1.get(col, '')
                dt2 = dtype_t2.get(col, '')
                e1 = _safe_dt_expr('t1', col, dt1)
                e2 = _safe_dt_expr('t2', col, dt2)
                select_columns.append(f"COALESCE({e1}, {e2}) AS `{col}`")
                rename_map_t1[col] = col
            else:
                dt = dtype_t1.get(col, '')
                e = _safe_dt_expr('t1', col, dt)
                select_columns.append(f"{e} AS `{col}`")
                rename_map_t1[col] = col
            seen_columns.add(col)

        # t2 列映射（冲突列加 _2 后缀；unify 列跳过，因为已由 t1 统一），
        # 同样对日期/时间列应用零日期安全包装。
        rename_map_t2 = {}
        for col in self.old_columns_list[1]:
            if col in unify_cols:
                # 该列在新表中使用 t1 的同名列
                rename_map_t2[col] = col
                continue
            if col in seen_columns:
                new_col_name = f"{col}_2"
            else:
                new_col_name = col
            dt = dtype_t2.get(col, '')
            e = _safe_dt_expr('t2', col, dt)
            select_columns.append(f"{e} AS `{new_col_name}`")
            rename_map_t2[col] = new_col_name

        # 连接条件
        if join_pairs:
            join_condition = " AND ".join([f"t1.{a} = t2.{b}" for (a, b) in join_pairs])
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
        # 将 join_key 对应列补充到主键集合（若未统一，分别加入 t1/t2 的映射后列名）
        if join_pairs:
            for (a, b) in join_pairs:
                # 统一列（a==b）只加入一次
                if a == b:
                    name = rename_map_t1.get(a) or rename_map_t2.get(b)
                    if name and name not in new_pk_cols:
                        new_pk_cols.append(name)
                else:
                    name1 = rename_map_t1.get(a)
                    name2 = rename_map_t2.get(b)
                    if name1 and name1 not in new_pk_cols:
                        new_pk_cols.append(name1)
                    if name2 and name2 not in new_pk_cols:
                        new_pk_cols.append(name2)
        if new_pk_cols:
            stmts.append(
                f"ALTER TABLE `{newt}` ADD PRIMARY KEY (" + ", ".join(f"`{c}`" for c in new_pk_cols) + ")"
            )

        # 复制唯一约束/出站外键（来自 t1 和 t2）
        include_t1 = list(rename_map_t1.values())
        include_t2 = [v for (k, v) in rename_map_t2.items() if k not in unify_cols]
        # t1（避免重复添加主键；为约束添加前缀名以规避跨表重名）
        stmts.extend(
            helper.build_add_constraints_for_table(
                newt,
                c1,
                include_t1,
                rename_map=rename_map_t1,
                skip_primary_key=True,
                name_prefix=f"{newt}_t1"
            )
        )
        # t2（同上）
        stmts.extend(
            helper.build_add_constraints_for_table(
                newt,
                c2,
                include_t2,
                rename_map=rename_map_t2,
                skip_primary_key=True,
                name_prefix=f"{newt}_t2"
            )
        )

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
        ok = True
        for s in stmts:
            ok = ok and db.execute_statement(s)
        return ok
        

    def create_physical_view(self, db):
        # 旧表保留
        """
        创建两个旧表自然连接的物化视图（MySQL中就是创建一个新表）。
        若两个表没有公共列，则优先使用 join_key 进行连接；
        若未提供有效的 join_key，则退化为笛卡尔积并给出提示。
        """
        # 找出两个表的公共列（自然连接的连接键）
        table1_cols = set(self.old_columns_list[0])
        table2_cols = set(self.old_columns_list[1])
        common_columns = list(table1_cols.intersection(table2_cols))
        join_key = getattr(self, 'join_key', None)
        
        if not common_columns:
            # 没有公共列：优先使用 join_key 进行等值连接，并据此去除“语义重复列”
            join_condition = None
            pairs = []
            if join_key:
                # 支持：'id' | ('t1_col','t2_col') | [(a,b),...]
                if isinstance(join_key, (list, tuple)):
                    if len(join_key) == 2 and all(not isinstance(x, (list, tuple)) for x in join_key):
                        pairs = [(join_key[0], join_key[1])]
                    else:
                        pairs = [(a, b) for (a, b) in join_key]
                elif isinstance(join_key, str):
                    if join_key in table1_cols and join_key in table2_cols:
                        pairs = [(join_key, join_key)]
            # 构造连接条件
            if pairs:
                join_condition = " AND ".join([f"t1.{a} = t2.{b}" for (a, b) in pairs])
            else:
                print(
                    f"警告：两个表没有公共列，提供的 join_key 形式或列名无法匹配（join_key={join_key!r}）；将使用笛卡尔积连接"
                )
                join_condition = "1=1"
            
            # 构建SELECT列表，处理重复列名
            select_columns = []
            
            # 第一个表的所有列
            for col in self.old_columns_list[0]:
                select_columns.append(f"t1.{col}")
            
            # 第二个表的所有列
            # 根据 join_key 对 (a,b)，若 a!=b，视为语义重复：仅保留 t1.a，跳过 t2.b
            drop_t2 = {b for (a, b) in pairs if a != b}
            for col in self.old_columns_list[1]:
                if col in drop_t2:
                    continue
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

            tables = [t.strip() for t in tables_part.split(',') if t.strip()]
            new_tables = []
            replaced = False

            for t in tables:
                parts = t.split()
                if not parts:
                    continue
                base_table = parts[0]
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
        import sqlglot
        from sqlglot import expressions as exp
        # 标准化源表短名
        t1_base = self.old_tables[0].split('.')[-1]
        t2_base = self.old_tables[1].split('.')[-1] if len(self.old_tables) > 1 else None
        old_tables = set([self.old_tables[0], self.old_tables[0].split('.')[-1]])
        if t2_base:
            old_tables.update([self.old_tables[1], t2_base])

        # 预处理：用正则把 FROM 列表中的源表替换为 new_table，并尽量保留一个别名
        pattern = re.compile(
            r'(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)',
            re.IGNORECASE | re.DOTALL
        )

        alias_keep = None
        # 记录各项的别名→基表短名，用于后续列/谓词清理
        alias_to_base: dict[str, str] = {}

        def _extract_alias(item: str) -> tuple[str | None, str]:
            # 返回 (alias or None, base_name_without_schema)
            parts = item.split()
            base = (parts[0] if parts else '').split('.')[-1]
            alias = None
            m = re.match(r"^([^\s]+)(?:\s+AS\s+|\s+)(\w+)\s*$", item, re.IGNORECASE)
            if m:
                alias = m.group(2)
            return alias, base

        def replace_from(match):
            nonlocal alias_keep, alias_to_base
            prefix = match.group(1)
            tables_part = match.group(2)

            tables = [t.strip() for t in tables_part.split(',') if t.strip()]
            # 判断是否同时包含两个源表（按短名判断）
            bases = [t.split()[0].split('.')[-1] for t in tables if t.split()]
            if t2_base and not ({t1_base, t2_base}.issubset(set(bases))):
                return match.group(0)

            new_tables = []
            replaced = False

            for t in tables:
                if not t:
                    continue
                alias, base = _extract_alias(t)
                if alias:
                    alias_to_base[alias.lower()] = base.lower()
                # 命中源表
                if base in (t1_base, t2_base):
                    if not replaced:
                        # 选择第一个出现的别名作为保留别名
                        alias_keep = alias or base
                        if alias_keep and alias_keep.lower() != new_table_name.lower():
                            new_tables.append(f"{new_table_name} AS {alias_keep}")
                        else:
                            new_tables.append(new_table_name)
                        replaced = True
                    # 其余源表项删除
                else:
                    new_tables.append(t)

            return prefix + ', '.join(new_tables)

        replaced_sql = pattern.sub(replace_from, sql)

        # 后处理：
        # - 将对被删除别名/表的列引用改写为保留别名/表；
        # - 将 join_key 的等值谓词从 WHERE 中移除；
        # - 将被删除表别名上的 join 列改为新表中的保留列名。

        # 归一化 join_pairs (t1_col, t2_col)
        pairs: list[tuple[str, str]] = []
        jk = self.join_key
        if jk:
            if isinstance(jk, (list, tuple)):
                if len(jk) == 2 and all(not isinstance(x, (list, tuple)) for x in jk):
                    pairs = [(jk[0], jk[1])]
                else:
                    pairs = [(a, b) for (a, b) in jk]
            elif isinstance(jk, str):
                pairs = [(jk, jk)]

        # 未能解析别名时，直接返回替换后的 SQL
        try:
            tree = sqlglot.parse_one(replaced_sql)
        except Exception:
            return replaced_sql

        t1_names = {t1_base}
        t2_names = {t2_base} if t2_base else set()
        # 收集所有在原 FROM 中出现过的别名（通过前面替换阶段记录）
        t1_aliases = {a for a, b in alias_to_base.items() if b == t1_base.lower()}
        t2_aliases = {a for a, b in alias_to_base.items() if b == (t2_base or '').lower()}
        # 将新表保留的别名归一化
        alias_keep_l = (alias_keep or '').lower() or None

        # 1) 列引用改写（把 t2 别名/表前缀改成 alias_keep；t2 的 join 列名改为 t1 列名）
        for col in list(tree.find_all(exp.Column)):
            name = col.name
            tbl = col.table
            if tbl:
                pref = tbl.split('.')[-1].lower()
                # t2 → alias_keep
                if pref in t2_aliases or pref in t2_names:
                    if alias_keep_l:
                        col.set("table", exp.to_identifier(alias_keep))
                    else:
                        col.set("table", None)
                    # 映射 join 列名 b → a
                    for a, b in pairs:
                        if name == b:
                            col.set("this", exp.to_identifier(a))
                            break
                # t1 → alias_keep（仅当保留别名与 t1 名不同）
                elif pref in t1_aliases or pref in t1_names:
                    if alias_keep_l and pref != alias_keep_l:
                        col.set("table", exp.to_identifier(alias_keep))
            else:
                # 无前缀列：若名称命中 t2 侧的 join 列名，则改为 t1 侧列名
                for a, b in pairs:
                    if name == b:
                        col.set("this", exp.to_identifier(a))
                        break

        # 2) 去除 join 等值条件：形如 t1.a = t2.b 或 t2.b = t1.a
        def is_old_join_eq(node: exp.Expression) -> bool:
            if not isinstance(node, exp.EQ):
                return False
            l, r = node.left, node.right
            if not (isinstance(l, exp.Column) and isinstance(r, exp.Column)):
                return False
            ltbl = (l.table or '').split('.')[-1].lower() if l.table else ''
            rtbl = (r.table or '').split('.')[-1].lower() if r.table else ''
            lname, rname = l.name, r.name
            pair_set = set(pairs)
            # 左 t1.a 右 t2.b 或 左 t2.b 右 t1.a
            if ((ltbl in t1_aliases or ltbl in t1_names) and (rtbl in t2_aliases or rtbl in t2_names) and (lname, rname) in pair_set):
                return True
            if ((ltbl in t2_aliases or ltbl in t2_names) and (rtbl in t1_aliases or rtbl in t1_names) and (rname, lname) in pair_set):
                return True
            return False

        def collect_conj(node: exp.Expression):
            items = []
            def walk(n: exp.Expression):
                if isinstance(n, exp.And):
                    walk(n.left)
                    walk(n.right)
                else:
                    items.append(n)
            walk(node)
            return items

        def rebuild_and(items: list[exp.Expression]):
            if not items:
                return None
            cur = items[0]
            for n in items[1:]:
                cur = exp.And(this=cur, expression=n)
            return cur

        where_node = tree.args.get('where')
        if where_node is not None and where_node.this is not None:
            conj = collect_conj(where_node.this)
            conj2 = [n for n in conj if not is_old_join_eq(n)]
            new_where = rebuild_and(conj2)
            if new_where is None:
                # 移除 WHERE 子句
                tree.set('where', None)
            else:
                where_node.set('this', new_where)

        return tree.sql()


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
    merger = TableJoin(
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

    
