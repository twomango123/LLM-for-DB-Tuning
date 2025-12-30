try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  # pragma: no cover
    from base import SMO, MySQLConstraintHelper

import re
import sqlglot
from sqlglot import expressions as exp


class RedundantColumnDrop(SMO):
    """
    删除冗余列：从 target_table 删除 redundant_column。
    - 自动源表探测：若未显式提供 source_table/source_column/join_keys，
      将通过 target_table 的出站外键自动推断：
        1) 选择一个外键引用的表作为可能的源表；
        2) 在该表中查找与冗余列名相同或后缀相同的列作为源列；
        3) 用外键列对作为 join_keys（child_col -> ref_col）。
    - apply_to_schema: 清理与该列相关的唯一/检查/外键后，DROP COLUMN；
      同时尝试自动探测并缓存映射，供 apply_to_sql 改写使用。
    - apply_to_sql: 将 target_table.redundant_column 改写为 source_table.source_column；
      若查询中尚未包含源表，则基于 join_keys 自动加入 JOIN 子句。
    """

    def __init__(self, target_table: str, redundant_column: str,
                 source_table: str | None = None, source_column: str | None = None,
                 join_keys: list[tuple[str, str]] | None = None):
        self.target_table = target_table.split('.')[-1]
        self.redundant_column = redundant_column
        self.source_table = source_table.split('.')[-1] if source_table else None
        self.source_column = source_column
        self.join_keys = join_keys or []

    def apply_to_schema(self, db=None):
        t = self.target_table
        c = self.redundant_column
        stmts = []
        stmts.append('SET FOREIGN_KEY_CHECKS=0')
        # 删除可能存在的涉及该列的索引/唯一约束/出站外键
        helper = MySQLConstraintHelper(db) if db is not None else None
        if helper:
            cons = helper.fetch_constraints(t)
            # 尝试自动探测映射（仅当尚未提供）
            if not (self.source_table and self.source_column and self.join_keys):
                try:
                    self._autodetect_mapping(db, cons)
                except Exception:
                    pass
            # 出站外键
            for fk in cons.get('foreign_keys_outbound', []) or []:
                child_cols = [x for (x,_,_) in fk['cols']]
                if c in child_cols:
                    stmts.append(f"ALTER TABLE `{t}` DROP FOREIGN KEY `{fk['constraint_name']}`")
            # 唯一约束
            for u in cons.get('uniques', []) or []:
                if c in (u.get('columns') or []):
                    stmts.append(f"ALTER TABLE `{t}` DROP INDEX `{u['name']}`")
            # CHECK 约束（涉及该列时需要删除）
            for ck in cons.get('checks', []) or []:
                import re as _re
                if _re.search(rf"(?<!\.)\b{c}\b", ck.get('clause') or '', _re.IGNORECASE):
                    # MySQL 8.0+: DROP CHECK name
                    stmts.append(f"ALTER TABLE `{t}` DROP CHECK `{ck['name']}`")
        # 删除列
        stmts.append(f"ALTER TABLE `{t}` DROP COLUMN `{c}`;")
        stmts.append('SET FOREIGN_KEY_CHECKS=1')
        if db is not None and hasattr(db, 'execute_statement'):
            ok = True
            for s in stmts:
                ok = ok and db.execute_statement(s)
            return ok
        return "\n".join(stmts)

    def apply_to_sql(self, sql: str) -> str:
        # 若没有可替代表达式，则不动 SQL（避免破坏查询）
        if not (self.source_table and self.source_column and self.join_keys):
            return sql

        # 1) 替换 target_table.redundant_column -> source_table.source_column
        try:
            tree = sqlglot.parse_one(sql)
            replaced = False
            for col in list(tree.find_all(exp.Column)):
                if col.table and col.table.lower() == self.target_table.lower() and col.name == self.redundant_column:
                    col.set("this", exp.to_identifier(self.source_column))
                    col.set("table", exp.to_identifier(self.source_table))
                    replaced = True
            new_sql = tree.sql() if replaced else sql
        except Exception:
            pattern = re.compile(rf"\b{re.escape(self.target_table)}\.{re.escape(self.redundant_column)}\b", re.IGNORECASE)
            new_sql = pattern.sub(f"{self.source_table}.{self.source_column}", sql)

        # 2) 确保 FROM 中包含源表；若缺失则添加 JOIN ... ON <target.child = source.ref>
        def contains_table(s: str, tbl: str) -> bool:
            return re.search(rf"\b{re.escape(tbl)}\b", s, re.IGNORECASE) is not None

        if not contains_table(new_sql, self.source_table):
            new_sql = self._add_source_table_join(new_sql)

        return new_sql

    def apply_to_data(self, row):
        return row

    # ---------- helpers ----------
    def _autodetect_mapping(self, db, cons=None):
        """基于出站外键自动探测 (source_table, source_column, join_keys)。"""
        helper = MySQLConstraintHelper(db)
        if cons is None:
            cons = helper.fetch_constraints(self.target_table)
        fks = cons.get('foreign_keys_outbound', []) or []
        if not fks:
            return False

        # 查询 referenced 表的列名集合
        def columns_of(table: str):
            rows = db.execute_query(
                f"""
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'
                """
            )
            return [r['COLUMN_NAME'] for r in rows]

        rc = self.redundant_column
        # 尝试每个外键的 referenced 表
        for fk in fks:
            ref_table = fk['cols'][0][1] if fk['cols'] else None
            if not ref_table:
                continue
            ref_cols_all = set(columns_of(ref_table)) if hasattr(db, 'execute_query') else set()
            # 先尝试完全同名匹配
            cand = None
            if rc in ref_cols_all:
                cand = rc
            else:
                # 允许后缀匹配：冗余列可能是 <prefix>_<refcol>
                for refc in ref_cols_all:
                    if rc.lower().endswith('_' + refc.lower()):
                        cand = refc
                        break
            if cand:
                self.source_table = ref_table
                self.source_column = cand
                # join_keys: (source_key, target_key)
                self.join_keys = [(refc, child) for (child, _, refc) in fk['cols']]
                return True
        return False

    def _add_source_table_join(self, sql: str) -> str:
        """在 FROM 子句中为 source_table 加入 JOIN ON 条件。"""
        if not (self.source_table and self.join_keys):
            return sql

        # 捕获 FROM ... 到 WHERE/ORDER/GROUP/UNION/)/结尾 的片段
        pattern = re.compile(
            r"(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)",
            re.IGNORECASE | re.DOTALL,
        )

        def repl(m):
            prefix, tables_part = m.group(1), m.group(2)
            # 拆分逗号分隔项（不深入处理 JOIN 子句）
            items = [t.strip() for t in tables_part.split(',')]
            # 查找目标表项与别名
            target_idx = -1
            target_item = None
            alias = self.target_table
            for i, it in enumerate(items):
                if re.search(rf"\b{re.escape(self.target_table)}\b", it, re.IGNORECASE):
                    target_idx = i
                    target_item = it
                    m2 = re.search(rf"\b{re.escape(self.target_table)}\b\s+(\w+)", it, re.IGNORECASE)
                    if m2:
                        alias = m2.group(1)
                    break
            if target_idx == -1:
                return m.group(0)

            # 构造 JOIN 语句
            on_parts = [f"{alias}.`{tkey}` = {self.source_table}.`{skey}`" for (skey, tkey) in self.join_keys]
            join_clause = f"{target_item} JOIN {self.source_table} ON " + " AND ".join(on_parts)
            items[target_idx] = join_clause
            return prefix + ', '.join(items)

        return pattern.sub(repl, sql)
