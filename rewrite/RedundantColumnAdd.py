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
        # 归一化 join_keys 到 [(source_key, target_key), ...]
        self.join_keys = self._normalize_join_keys(join_keys)

    @staticmethod
    def _normalize_join_keys(join_keys):
        pairs = []
        if not join_keys:
            return pairs
        # 单字符串：同名列
        if isinstance(join_keys, str):
            return [(join_keys, join_keys)]
        # 二元组：一对列
        if isinstance(join_keys, (tuple, list)) and len(join_keys) == 2 and not isinstance(join_keys[0], (tuple, list)) and not isinstance(join_keys[1], (tuple, list)):
            return [(str(join_keys[0]), str(join_keys[1]))]
        # 列表：若元素为字符串或序列，取前两个作为 (src, tgt)
        try:
            for item in join_keys:
                if isinstance(item, str):
                    pairs.append((item, item))
                elif isinstance(item, (tuple, list)):
                    if len(item) >= 2:
                        pairs.append((str(item[0]), str(item[1])))
                # 其它形式忽略
        except Exception:
            pass
        return pairs

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
        将 source_table.source_column → target_table.new_column；
        仅当“原查询中出现的源表列全部已被目标表冗余覆盖”时，才移除 FROM 中的源表及其等值连接条件。
        - 支持 schema 前缀与别名；
        - 连接条件仅移除基于 join_keys 的等值谓词；
        - 无法完整覆盖时，仅做列名替换，不移除源表。
        """
        try:
            tree = sqlglot.parse_one(sql, read='mysql')
        except Exception:
            # 解析失败：仅做稳妥替换，不尝试移除源表
            pattern = re.compile(rf"\b{re.escape(self.source_table)}\.{re.escape(self.source_column)}\b", re.IGNORECASE)
            return pattern.sub(f"{self.target_table}.{self.new_column}", sql)

        # 收集 FROM 中的表别名映射（alias -> base）
        alias_to_base: dict[str, str] = {}
        target_aliases: set[str] = set()
        source_aliases: set[str] = set()
        src_base = self.source_table.lower()
        tgt_base = self.target_table.lower()

        for tbl in list(tree.find_all(exp.Table)):
            full = tbl.name or ''
            base = full.split('.')[-1].lower() if full else ''
            alias = None
            if tbl.args.get('alias') is not None:
                alias = tbl.args['alias'].sql(dialect='mysql').split()[-1]
            tok = (alias or base or '').lower()
            if not tok:
                continue
            alias_to_base[tok] = base
            if base == src_base:
                source_aliases.add(tok)
            if base == tgt_base:
                target_aliases.add(tok)

        # 步骤1：替换对源表中冗余列的引用 → 目标表对应新列
        # 选择目标前缀：优先用唯一的目标别名；否则用目标基表名；若都无则去前缀
        if len(target_aliases) == 1:
            tgt_prefix = next(iter(target_aliases))
        elif tgt_base in alias_to_base.values():
            tgt_prefix = self.target_table
        else:
            tgt_prefix = None

        for col in list(tree.find_all(exp.Column)):
            t = (col.table or '').split('.')[-1].lower() if col.table else ''
            if t and t in source_aliases and col.name == self.source_column:
                col.set("this", exp.to_identifier(self.new_column))
                if tgt_prefix:
                    col.set("table", exp.to_identifier(tgt_prefix))
                else:
                    col.set("table", None)

        # 步骤2：检查是否还有源表列被引用（除已冗余的列外）
        remaining_src_cols: set[str] = set()
        for col in list(tree.find_all(exp.Column)):
            t = (col.table or '').split('.')[-1].lower() if col.table else ''
            if t and t in source_aliases:
                remaining_src_cols.add(col.name)

        # 若仍有其他源表列被引用，则不能移除源表；仅返回列替换后的 SQL
        if remaining_src_cols:
            return tree.sql(dialect='mysql')

        # 步骤3：移除基于 join_keys 的等值连接条件（source_alias.skey = X.tkey 或反向）
        pairs = set(tuple(p) for p in (self.join_keys or []))

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

        def is_join_eq(n: exp.Expression) -> bool:
            if not isinstance(n, exp.EQ):
                return False
            l, r = n.left, n.right
            if not (isinstance(l, exp.Column) and isinstance(r, exp.Column)):
                return False
            lt = (l.table or '').split('.')[-1].lower() if l.table else ''
            rt = (r.table or '').split('.')[-1].lower() if r.table else ''
            if lt in source_aliases and (l.name, r.name) in pairs:
                return True
            if rt in source_aliases and (r.name, l.name) in pairs:
                return True
            return False

        where_node = tree.args.get('where')
        if where_node is not None and where_node.this is not None:
            conj = collect_conj(where_node.this)
            conj2 = [n for n in conj if not is_join_eq(n)]
            new_where = rebuild_and(conj2)
            if new_where is None:
                tree.set('where', None)
            else:
                where_node.set('this', new_where)

        # 步骤4：从 FROM 中移除源表（仅一次）；保留其它项与别名
        sql_after = tree.sql(dialect='mysql')

        pattern = re.compile(
            r"(from\s+)([^;]+?)(?=\s+where|\s+group|\s+order|\s+union|\)|$)",
            re.IGNORECASE | re.DOTALL,
        )

        def repl(m):
            prefix, tables = m.group(1), m.group(2)
            parts = [p.strip() for p in tables.split(',') if p.strip()]
            removed = False
            new_parts = []
            for p in parts:
                first = p.split()[0] if p.split() else ''
                base = first.split('.')[-1]
                # 捕获别名
                alias = None
                m2 = re.match(r"^([^\s]+)(?:\s+AS\s+|\s+)(\w+)\s*$", p, re.IGNORECASE)
                if m2:
                    alias = m2.group(2).lower()
                if not removed and (
                    base.lower() == self.source_table.lower() or (alias and alias in source_aliases)
                ):
                    removed = True
                    continue
                new_parts.append(p)
            return prefix + (', '.join(new_parts) if new_parts else tables)

        return pattern.sub(repl, sql_after)

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
