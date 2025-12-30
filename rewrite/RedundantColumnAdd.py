try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  # pragma: no cover
    from base import SMO, MySQLConstraintHelper

import re
import sqlglot
from sqlglot import expressions as exp


class RedundantColumnAdd(SMO):
    """
    在 target_table 中增加冗余列：将 source_table.source_column 复制到
    target_table.new_column（通过 join_keys 对齐行）。

    - join_keys: 列映射列表 [(source_key, target_key), ...]
    - apply_to_schema: CTAS + RENAME + 清理
    - apply_to_sql: 将 SQL 中对 source_table.source_column 的引用替换为
      target_table.new_column。如果替换后源表不再被引用，尝试从 FROM 中移除该表。
    """

    def __init__(self, source_table: str, source_column: str,
                 target_table: str, new_column: str,
                 join_keys: list[tuple[str, str]]):
        self.source_table = source_table.split('.')[-1]
        self.source_column = source_column
        self.target_table = target_table.split('.')[-1]
        self.new_column = new_column
        self.join_keys = join_keys or []

    # ---------- schema ----------
    def apply_to_schema(self, db=None):
        s = self.source_table
        t = self.target_table
        sc = self.source_column
        nc = self.new_column

        if not self.join_keys:
            raise ValueError("RedundantColumnAdd 需要 join_keys 指定行对齐关系")

        on_clause = " AND ".join([f"t.`{tkey}` = s.`{skey}`" for (skey, tkey) in self.join_keys])

        stmts = [
            'SET FOREIGN_KEY_CHECKS=0',
            f"CREATE TABLE `{t}__tmp_rca` AS SELECT t.*, s.`{sc}` AS `{nc}` FROM `{t}` t LEFT JOIN `{s}` s ON {on_clause};",
            f"RENAME TABLE `{t}` TO `{t}__old_rca`, `{t}__tmp_rca` TO `{t}`;",
            f"DROP TABLE `{t}__old_rca`;",
        ]
        # 复制旧表上的主键/唯一/出站外键/默认/自增
        helper = MySQLConstraintHelper(db) if db is not None else None
        if helper:
            cons = helper.fetch_constraints(t)
            include_cols = [c['COLUMN_NAME'] for c in (cons.get('columns') or [])]
            stmts.extend(helper.build_add_constraints_for_table(t, cons, include_cols, rename_map=None))
            # 默认/自增
            def _lit(v: str):
                if v is None or v == 'NULL':
                    return 'NULL'
                try:
                    float(v)
                    return v
                except Exception:
                    pass
                return "'" + str(v).replace("'","''") + "'"
            for cm in cons.get('columns') or []:
                col = cm['COLUMN_NAME']
                default = cm.get('COLUMN_DEFAULT')
                extra = (cm.get('EXTRA') or '').lower()
                ctype = cm.get('COLUMN_TYPE') or 'varchar(255)'
                nullable = cm.get('IS_NULLABLE','YES')
                if default is not None:
                    stmts.append(f"ALTER TABLE `{t}` ALTER COLUMN `{col}` SET DEFAULT {_lit(str(default))}")
                if 'auto_increment' in extra:
                    stmts.append(f"ALTER TABLE `{t}` MODIFY COLUMN `{col}` {ctype} {'NOT NULL' if nullable=='NO' else 'NULL'} AUTO_INCREMENT")

        stmts.append('SET FOREIGN_KEY_CHECKS=1')
        script = "\n".join(stmts)
        if db is not None and hasattr(db, "execute_statement"):
            ok = True
            for ssql in stmts:
                ok = ok and db.execute_statement(ssql)
            return ok
        return script

    # ---------- sql ----------
    def apply_to_sql(self, sql: str) -> str:
        """
        将 source_table.source_column -> target_table.new_column。
        若源表随后不再被引用，尝试从 FROM 中去掉一个出现（仅限逗号分隔场景）。
        """
        try:
            tree = sqlglot.parse_one(sql)
        except Exception:
            # Fallback: 正则替换列名
            pattern = re.compile(rf"\b{re.escape(self.source_table)}\.{re.escape(self.source_column)}\b", re.IGNORECASE)
            sql2 = pattern.sub(f"{self.target_table}.{self.new_column}", sql)
            return self._try_remove_source_table(sql2)

        # 精确列替换
        for col in list(tree.find_all(exp.Column)):
            if col.table and col.table.lower() == self.source_table.lower() and col.name == self.source_column:
                col.set("this", exp.to_identifier(self.new_column))
                col.set("table", exp.to_identifier(self.target_table))

        rewritten = tree.sql()
        return self._try_remove_source_table(rewritten)

    def _try_remove_source_table(self, sql: str) -> str:
        # 若仍有 source_table. 的引用，直接返回
        if re.search(rf"\b{re.escape(self.source_table)}\.", sql, re.IGNORECASE):
            return sql

        # 仅处理 FROM a, b, c ... 的简单情况，去除一个源表出现
        pattern = re.compile(
            r"(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)",
            re.IGNORECASE | re.DOTALL,
        )

        def repl(m):
            prefix, tables = m.group(1), m.group(2)
            parts = [p.strip() for p in tables.split(',')]
            removed = False
            new_parts = []
            for p in parts:
                base = p.split()[0]
                if base.split('.')[-1].lower() == self.source_table.lower() and not removed:
                    removed = True
                else:
                    new_parts.append(p)
            return prefix + (', '.join(new_parts) if new_parts else tables)

        return pattern.sub(repl, sql)

    # ---------- data ----------
    def apply_to_data(self, row):
        return row
