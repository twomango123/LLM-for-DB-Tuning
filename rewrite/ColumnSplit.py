try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  
    from base import SMO, MySQLConstraintHelper

import sqlglot
from sqlglot import parse_one
from sqlglot import expressions as exp


class ColumnSplit(SMO):
    def __init__(self, table, old_column, new_columns, split_delimiter=None, split_position=None):
        """
        将单列拆分为多列。
        - 如果提供 split_delimiter，则按分隔符拆分。
        - 如果提供 split_position(int, 仅支持两段)，则按固定位置拆分。

        Args:
            table: 表名
            old_column: 原列名
            new_columns: 新列名列表
            split_delimiter: 分隔符（优先）
            split_position: 固定切分位置，仅支持两段
        """
        self.table = table
        self.old_column = old_column
        self.new_columns = list(new_columns)
        self.split_delimiter = split_delimiter
        self.split_position = split_position

    def _mysql_expr_for_part(self, idx: int) -> str:
        """返回 MySQL 下第 idx 个分段的表达式 (1-based)。"""
        d = self.split_delimiter
        oc = f"`{self.old_column}`"
        # SUBSTRING_INDEX(SUBSTRING_INDEX(col, d, idx), d, -1)
        return f"SUBSTRING_INDEX(SUBSTRING_INDEX({oc}, '{d}', {idx}), '{d}', -1)"

    def apply_to_schema(self, db=None):
        """
        MySQL 专用：属性拆分的 schema 变更，遵循如下规则：
        - 若被拆分列具有 自增/检查/唯一 约束：禁止拆分，直接报错；
        - 若被拆分列是主键的一部分：重建主键，将该列替换为拆分后的多列；
        - 若该列被其他表通过外键引用：为子表按相同拆分规则新增对应列，重建外键；
        - 若该列有默认值：按拆分规则拆分默认值，并为新列设置默认值；
        - 最终通过 CTAS - RENAME - DROP COLUMN 完成数据迁移。

        返回：db 存在则执行并返回 bool；否则返回完整 SQL 脚本字符串。
        """
        t = self.table
        old = self.old_column

        helper = MySQLConstraintHelper(db) if db is not None else None
        constraints = helper.fetch_constraints(t) if helper else {'columns': []}

        def execs(statements):
            if db is not None and hasattr(db, 'execute_statement'):
                ok = True
                for x in statements:
                    ok = ok and db.execute_statement(x)
                return ok
            return "\n".join(statements)

        # 读取列元数据
        col_meta = None
        for cm in constraints.get('columns', []):
            if cm['COLUMN_NAME'] == old:
                col_meta = cm
                break

        # 基本校验
        if col_meta:
            extra = (col_meta.get('EXTRA') or '').lower()
            if 'auto_increment' in extra:
                raise ValueError(f"不允许拆分：列 `{old}` 含自增约束")

        # 唯一约束不允许拆分
        for u in constraints.get('uniques', []) or []:
            if old in (u.get('columns') or []):
                raise ValueError(f"不允许拆分：列 `{old}` 参与唯一约束 `{u['name']}`")

        # 检查约束不允许拆分（简单判断表达式中是否出现列名）
        for ck in constraints.get('checks', []) or []:
            clause = ck.get('clause') or ''
            # 词边界匹配，避免误伤
            import re as _re
            if _re.search(rf"\b{old}\b", clause, _re.IGNORECASE):
                raise ValueError(f"不允许拆分：列 `{old}` 参与 CHECK 约束 `{ck['name']}`")

        # 出站外键：当前实现不支持拆分作为外键引用其他表的列
        for fk in constraints.get('foreign_keys_outbound', []) or []:
            child_cols = [c for (c, _, _) in fk['cols']]
            if old in child_cols:
                raise ValueError(f"不允许拆分：列 `{old}` 作为外键引用其他表 (约束 {fk['constraint_name']})")

        # 是否在主键中
        pk = constraints.get('primary_key')
        old_in_pk = bool(pk and old in pk.get('columns', []))

        # 预处理：入站外键（其他表引用本列）——提前删除外键，后续在子表添加拆分列并重建
        inbound_related = []
        for fk in constraints.get('foreign_keys_inbound', []) or []:
            # 如果该外键涉及到对本列的引用
            refs = [ref for (_, ref) in fk['cols']]
            if old in refs:
                inbound_related.append(fk)

        stmts: list[str] = []
        stmts.append('SET FOREIGN_KEY_CHECKS=0')

        # 1) 移除相关入站外键
        for fk in inbound_related:
            stmts.append(f"ALTER TABLE `{fk['child_table']}` DROP FOREIGN KEY `{fk['constraint_name']}`")

        # 2) 子表新增对应拆分列 + 填充数据
        #    子列表达式沿用同样的拆分规则，基于原子表外键列进行拆分
        def split_expr_on(child_col: str, idx: int) -> str:
            if self.split_delimiter is not None:
                return f"SUBSTRING_INDEX(SUBSTRING_INDEX(`{child_col}`, '{self.split_delimiter}', {idx}), '{self.split_delimiter}', -1)"
            else:
                p = int(self.split_position)
                if idx == 1:
                    return f"SUBSTRING(`{child_col}`, 1, {p})"
                else:
                    return f"SUBSTRING(`{child_col}`, {p+1})"

        # child_col 新列命名策略：child_col + '_' + new_component_name（冲突时加序号）
        for fk in inbound_related:
            child = fk['child_table']
            # 找到该 FK 中对 old 的子表列
            replace_pairs = [(c, r) for (c, r) in fk['cols'] if r == old]
            for child_col, _ in replace_pairs:
                # 获取子表列类型（沿用原列类型）
                child_col_meta = []
                if db is not None and hasattr(db, 'execute_query'):
                    child_col_meta = db.execute_query(
                        f"""
                        SELECT COLUMN_TYPE, IS_NULLABLE
                        FROM information_schema.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                          AND TABLE_NAME = '{child}'
                          AND COLUMN_NAME = '{child_col}'
                        """
                    )
                col_type = child_col_meta[0]['COLUMN_TYPE'] if child_col_meta else 'varchar(255)'
                nullable = child_col_meta[0]['IS_NULLABLE'] if child_col_meta else 'YES'

                new_child_cols = []
                for i, nc in enumerate(self.new_columns, start=1):
                    base_name = f"{child_col}_{nc}"
                    name = base_name
                    # 避免重名
                    if db is not None and hasattr(db, 'execute_query'):
                        exists = db.execute_query(
                            f"""
                            SELECT 1 FROM information_schema.COLUMNS
                            WHERE TABLE_SCHEMA = DATABASE()
                              AND TABLE_NAME = '{child}'
                              AND COLUMN_NAME = '{name}'
                            LIMIT 1
                            """
                        )
                        if exists:
                            name = f"{base_name}_{i}"
                    new_child_cols.append(name)
                    stmts.append(
                        f"ALTER TABLE `{child}` ADD COLUMN `{name}` {col_type} {'NULL' if nullable=='YES' else 'NOT NULL'}"
                    )
                    stmts.append(
                        f"UPDATE `{child}` SET `{name}` = {split_expr_on(child_col, i)}"
                    )

                # 保存映射以便后续重建 FK
                fk.setdefault('_child_col_map', {})[child_col] = new_child_cols

        # 3) 父表：CTAS 添加新列并迁移数据
        # 生成新列表达式
        select_new_cols = []
        if self.split_delimiter is not None:
            for i, new_col in enumerate(self.new_columns, start=1):
                select_new_cols.append(f"{self._mysql_expr_for_part(i)} AS `{new_col}`")
        elif self.split_position is not None:
            if len(self.new_columns) != 2 or not isinstance(self.split_position, int):
                raise ValueError("split_position 仅支持将列拆成两段，且应为 int")
            p = self.split_position
            c = f"`{old}`"
            select_new_cols.append(f"SUBSTRING({c}, 1, {p}) AS `{self.new_columns[0]}`")
            select_new_cols.append(f"SUBSTRING({c}, {p+1}) AS `{self.new_columns[1]}`")
        else:
            raise ValueError("必须提供 split_delimiter 或 split_position")

        stmts.append(
            f"CREATE TABLE `{t}__tmp_split` AS SELECT *, " + ", ".join(select_new_cols) + f" FROM `{t}`;"
        )
        stmts.append(f"RENAME TABLE `{t}` TO `{t}__old_split`, `{t}__tmp_split` TO `{t}`;")
        stmts.append(f"ALTER TABLE `{t}` DROP COLUMN `{old}`;")

        # 4) 重建主键（若旧列在主键中，则替换为新列集合）以及其它约束（不涉及旧列）
        include_cols = [c['COLUMN_NAME'] for c in (constraints.get('columns') or []) if c['COLUMN_NAME'] != old]
        include_cols += self.new_columns

        if pk and pk.get('columns'):
            if old_in_pk:
                # 按原顺序替换旧列为新列集合
                new_pk_cols: list[str] = []
                for col in pk['columns']:
                    if col == old:
                        new_pk_cols.extend(self.new_columns)
                    else:
                        new_pk_cols.append(col)
                stmts.append(f"ALTER TABLE `{t}` ADD PRIMARY KEY (" + ", ".join(f"`{c}`" for c in new_pk_cols) + ")")
            else:
                # 原主键保留
                stmts.append(f"ALTER TABLE `{t}` ADD PRIMARY KEY (" + ", ".join(f"`{c}`" for c in pk['columns']) + ")")

        # 其余约束（唯一、出站外键），仅当其列集合不包含旧列时迁移
        if helper:
            add_stmts = helper.build_add_constraints_for_table(t, constraints, include_cols, rename_map=None)
            stmts.extend(add_stmts)

        # 5) 默认值重建（若旧列有默认值且可拆分）
        if col_meta and col_meta.get('COLUMN_DEFAULT') is not None:
            def _lit(v: str) -> str:
                if v is None or v == 'NULL':
                    return 'NULL'
                # 简单判断数字
                try:
                    float(v)
                    return v
                except Exception:
                    pass
                v = str(v).replace("'", "''")
                return f"'{v}'"

            default_val = str(col_meta['COLUMN_DEFAULT'])
            defaults = []
            if self.split_delimiter is not None:
                parts = default_val.split(self.split_delimiter)
                # pad to length
                while len(parts) < len(self.new_columns):
                    parts.append('')
                defaults = parts[:len(self.new_columns)]
            else:
                p = int(self.split_position)
                if len(self.new_columns) != 2:
                    defaults = ['', '']
                else:
                    defaults = [default_val[:p], default_val[p:]]

            for nc, dv in zip(self.new_columns, defaults):
                stmts.append(f"ALTER TABLE `{t}` ALTER COLUMN `{nc}` SET DEFAULT {_lit(dv)}")

        # 6) 重建入站外键（用新增子表列替代旧映射）
        for fk in inbound_related:
            child = fk['child_table']
            child_cols_new: list[str] = []
            ref_cols_new: list[str] = []
            for child_col, ref_col in fk['cols']:
                if ref_col == old:
                    new_child_cols = fk.get('_child_col_map', {}).get(child_col, [])
                    child_cols_new.extend(f"`{c}`" for c in new_child_cols)
                    ref_cols_new.extend(f"`{c}`" for c in self.new_columns)
                else:
                    child_cols_new.append(f"`{child_col}`")
                    ref_cols_new.append(f"`{ref_col}`")

            clause = (
                f"ALTER TABLE `{child}` ADD CONSTRAINT `{fk['constraint_name']}` "
                f"FOREIGN KEY (" + ", ".join(child_cols_new) + ") "
                f"REFERENCES `{t}` (" + ", ".join(ref_cols_new) + ")"
            )
            if fk.get('delete_rule'):
                clause += f" ON DELETE {fk['delete_rule']}"
            if fk.get('update_rule'):
                clause += f" ON UPDATE {fk['update_rule']}"
            stmts.append(clause)

        stmts.append('SET FOREIGN_KEY_CHECKS=1')

        # 执行或返回脚本
        return execs(stmts)

    def apply_to_sql(self, sql: str) -> str:
        """
        将查询中对“目标表的旧列”的使用，按上下文改写为新列：
          - SELECT 投影：将单个旧列替换为多个新列（保留表别名/前缀）。
          - WHERE <op> <value>：若右侧为常量，按拆分规则将其拆分成多个值，并改写为
            new1 <op> v1 AND new2 <op> v2 (...)。
          - GROUP BY / ORDER BY：将旧列展开为多个新列键（ORDER 继承排序方向）。
          - JOIN ON：若检测到在 ON 谓词中引用旧列，则不改写 ON，并打印提示“建议不修改，会有 join 冲突”。

        说明：仅改写与目标表相关的列。无前缀列仅在该目标表在 FROM 中唯一时才改写。
        """

        def build_alias_maps(scope_select: exp.Select):
            alias_to_base: dict[str, str] = {}
            base_count: dict[str, int] = {}
            for tbl in list(scope_select.find_all(exp.Table)):
                base = (tbl.name or '').split('.')[-1].lower()
                if base:
                    base_count[base] = base_count.get(base, 0) + 1
                alias_node = tbl.args.get('alias')
                if alias_node is not None:
                    alias_name = getattr(alias_node, 'name', None)
                    if alias_name:
                        alias_to_base[alias_name.lower()] = base
            return alias_to_base, base_count

        def is_target_col(col: exp.Column, alias_to_base: dict[str, str], base_count: dict[str, int]) -> tuple[bool, str | None]:
            old_lower = self.old_column.lower()
            if (col.name or '').lower() != old_lower:
                return False, None
            target_base = self.table.split('.')[-1].lower()
            prefix = col.table
            if prefix:
                pref = prefix.split('.')[-1].lower()
                mapped = alias_to_base.get(pref, pref)
                return (mapped == target_base), prefix
            # 无前缀：仅当目标表在 FROM 中唯一出现时改写
            unique = base_count.get(target_base, 0) == 1
            return (unique and target_base in base_count), None

        def make_col(prefix: str | None, name: str) -> exp.Expression:
            if prefix:
                return parse_one(f"{prefix}.{name}")
            return parse_one(name)

        def warn_join_once():
            nonlocal join_warned
            if not join_warned:
                print("提示：JOIN ON 中出现被拆分列，建议不修改，会有 join 冲突")
                join_warned = True

        def split_value(lit: exp.Literal) -> list[str] | None:
            # 返回与新列一一对应的字符串值列表；不足部分以空串补齐
            raw = str(getattr(lit, 'this', ''))
            parts: list[str]
            if self.split_delimiter is not None:
                parts = raw.split(self.split_delimiter)
                if len(parts) < len(self.new_columns):
                    parts += [''] * (len(self.new_columns) - len(parts))
                return parts[:len(self.new_columns)]
            elif self.split_position is not None:
                # 仅支持两段
                p = int(self.split_position)
                a, b = raw[:p], raw[p:]
                parts = [a, b]
                if len(self.new_columns) == 2:
                    return parts
                # 若新列不为2，仅返回前两列对应的值，其余为空
                return (parts + [''] * (len(self.new_columns) - 2))[:len(self.new_columns)]
            return None

        def to_sql_literal(v: str, was_string: bool) -> str:
            if was_string:
                v = v.replace("'", "''")
                return f"'{v}'"
            # 数字或其它：直接返回（若不是纯数字将作为标识解析，调用方需保证数据类型正确）
            return v

        try:
            tree = sqlglot.parse_one(sql, read='postgres')

            join_warned = False

            # 遍历所有 SELECT 作用域进行改写
            for sel in list(tree.find_all(exp.Select)):
                alias_to_base, base_count = build_alias_maps(sel)

                # 1) JOIN ON 警告（不改写 ON 内部）
                for j in list(sel.find_all(exp.Join)):
                    on_expr = j.args.get('on')
                    if not on_expr:
                        continue
                    for c in on_expr.find_all(exp.Column):
                        ok, _ = is_target_col(c, alias_to_base, base_count)
                        if ok:
                            warn_join_once()
                            break

                # 2) WHERE： c <op> 'value' -> (new1 <op> v1 AND new2 <op> v2 ...)
                where_node = sel.args.get('where')
                if where_node is not None:
                    # 支持的二元比较运算节点类型
                    cmp_types = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE)
                    op_symbol = {
                        exp.EQ: "=",
                        exp.NEQ: "<>",
                        exp.GT: ">",
                        exp.GTE: ">=",
                        exp.LT: "<",
                        exp.LTE: "<=",
                    }
                    invert = {"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "=", "<>": "<>"}
                    for cmp in list(where_node.find_all(cmp_types)):
                        left = cmp.args.get('this')
                        right = cmp.args.get('expression')
                        # 左右翻转的情况
                        col_side = None
                        lit_side = None
                        reverse = False
                        if isinstance(left, exp.Column) and isinstance(right, exp.Literal):
                            col_side, lit_side = left, right
                        elif isinstance(right, exp.Column) and isinstance(left, exp.Literal):
                            col_side, lit_side = right, left
                            reverse = True
                        else:
                            continue

                        ok, prefix = is_target_col(col_side, alias_to_base, base_count)
                        if not ok:
                            continue

                        vals = split_value(lit_side)
                        if vals is None:
                            continue

                        # 构造 new_i <op> val_i 的 AND 串
                        op_sql = op_symbol.get(type(cmp), "=")
                        if reverse:
                            op_sql = invert.get(op_sql, op_sql)
                        conjuncts = []
                        for nc, v in zip(self.new_columns, vals):
                            left_expr = make_col(prefix, nc)
                            right_sql = to_sql_literal(v, bool(getattr(lit_side, 'is_string', False)))
                            expr_sql = f"{left_expr.sql(dialect='mysql')} {op_sql} {right_sql}"
                            conjuncts.append(parse_one(expr_sql, read='mysql'))
                        new_pred = conjuncts[0]
                        for nxt in conjuncts[1:]:
                            new_pred = exp.and_(new_pred, nxt)
                        cmp.replace(new_pred)

                # 3) GROUP BY：展开为多个新列
                group_node = sel.args.get('group')
                if group_node is not None:
                    new_items = []
                    for gexp in list(group_node.expressions):
                        if isinstance(gexp, exp.Column):
                            ok, prefix = is_target_col(gexp, alias_to_base, base_count)
                            if ok:
                                for nc in self.new_columns:
                                    new_items.append(make_col(prefix, nc))
                                continue
                        new_items.append(gexp)
                    group_node.set('expressions', new_items)

                # 4) ORDER BY：展开为多个新列，继承方向
                order_node = sel.args.get('order')
                if order_node is not None:
                    new_orders = []
                    for o in list(order_node.expressions):
                        target = o.this if hasattr(o, 'this') else None
                        if isinstance(target, exp.Column):
                            ok, prefix = is_target_col(target, alias_to_base, base_count)
                            if ok:
                                desc = bool(getattr(o, 'args', {}).get('desc'))
                                for nc in self.new_columns:
                                    oe = exp.Ordered(this=make_col(prefix, nc), desc=desc)
                                    new_orders.append(oe)
                                continue
                        new_orders.append(o)
                    order_node.set('expressions', new_orders)

                # 5) SELECT 投影：将独立旧列替换为多个新列
                #    仅当投影表达式为旧列（或列别名包装）时进行展开；
                #    对包含在复杂表达式中的旧列不在此处改写（避免语义不明）。
                new_projs = []
                for proj in list(sel.expressions):
                    node = proj
                    alias_node = None
                    if isinstance(proj, exp.Alias):
                        alias_node = proj.args.get('alias')
                        node = proj.this
                    if isinstance(node, exp.Column):
                        ok, prefix = is_target_col(node, alias_to_base, base_count)
                        if ok:
                            for nc in self.new_columns:
                                new_projs.append(make_col(prefix, nc))
                            continue
                    new_projs.append(proj)
                sel.set('expressions', new_projs)

            return tree.sql(dialect='mysql')
        except Exception:
            return sql

    
