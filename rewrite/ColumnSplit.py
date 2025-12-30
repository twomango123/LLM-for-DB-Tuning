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
        对只读 SQL：将对旧列的引用替换为 CONCAT_WS 重构表达式，
        以保持查询语义与拆分前一致。
        """
        def rebuild_expr(table_qualifier: str | None) -> exp.Expression:
            qualified_cols = [
                f"{table_qualifier}.{c}" if table_qualifier else c for c in self.new_columns
            ]
            if self.split_delimiter is not None:
                func_sql = (
                    f"CONCAT_WS('{self.split_delimiter}', "
                    + ", ".join(qualified_cols)
                    + ")"
                )
            else:
                # split_position 情形：两段直接 CONCAT 组回
                func_sql = "CONCAT(" + ", ".join(qualified_cols) + ")"
            return parse_one(func_sql)

        try:
            tree = sqlglot.parse_one(sql)
            for col in list(tree.find_all(exp.Column)):
                if col.name == self.old_column:
                    alias = col.table
                    new_node = rebuild_expr(alias)
                    col.replace(new_node)
            return tree.sql()
        except Exception:
            return sql

    def apply_to_data(self, row):
        return row
