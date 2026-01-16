from sqlglot import parse_one, exp
import re
try:
    # Prefer relative import when used as a package
    from .base import SMO
except Exception:  # pragma: no cover - fallback for script execution
    from base import SMO


# 列重命名
class ColumnRename(SMO):
    def __init__(self, table, old, new):
        self.table = table
        self.old = old
        self.new = new

    def apply_to_schema(self, db=None):
        """
        构造并执行最简 SQL：
          ALTER TABLE <table> RENAME COLUMN <old> TO <new>

        - 若提供 db（实现 execute_statement 或 execute），则直接执行并返回执行结果；
        - 若未提供 db，则返回 SQL 字符串，便于外部执行。

        备注：根据你的要求，这里不再处理外键、主键或其它约束的重建逻辑。
        """
        def qident(name: str) -> str:
            # 对标识符进行反引号包裹（若已包含则直接返回）
            if name.startswith('`') and name.endswith('`'):
                return name
            return f"`{name}`"

        sql = (
            f"ALTER TABLE {qident(self.table)} "
            f"RENAME COLUMN {qident(self.old)} TO {qident(self.new)}"
        )

        if db is None:
            return sql

        # 优先使用 execute_statement，其次尝试 execute
        if hasattr(db, 'execute_statement'):
            return db.execute_statement(sql)
        if hasattr(db, 'execute'):
            return db.execute(sql)

        # 无可用执行方法则返回 SQL
        return sql

    def apply_to_sql(self, sql: str) -> str:
        """
        把查询中对“指定表的旧列名”的引用改为新列名，尽量保留表前缀/别名，
        并更稳健地支持：
          - schema 前缀（如 tpcch.orders）
          - 表别名（FROM t AS x）
          - SELECT/WHERE/GROUP BY/ORDER BY 中的列引用（包括函数内）

        策略：
          1) 解析 FROM 中出现的表与别名，建立 alias->base_table 映射；
          2) 对于带前缀的列（table.col）：仅当该前缀解析到目标表时才改名；
          3) 对于无前缀列：仅当 FROM 中目标表唯一出现时改名（避免歧义误改）。
            （如需更激进的行为可在此基础上扩展）
        解析失败则回退到正则兜底（仅替换无前缀的旧列名）。
        """
        try:
            tree = parse_one(sql, read='mysql')

            # 收集当前语句中出现的基础表及别名映射，并统计同一基础表出现的次数
            alias_to_base: dict[str, str] = {}
            base_tables_count: dict[str, int] = {}

            for tbl in list(tree.find_all(exp.Table)):
                base = (tbl.name or '').split('.')[-1].lower()
                if base:
                    base_tables_count[base] = base_tables_count.get(base, 0) + 1
                alias_node = tbl.args.get('alias')
                if alias_node is not None:
                    # sqlglot 提供 TableAlias.name 取别名
                    alias_name = getattr(alias_node, 'name', None)
                    if alias_name:
                        alias_to_base[alias_name.lower()] = base

            target_base = self.table.split('.')[-1].lower()
            target_unique = base_tables_count.get(target_base, 0) == 1

            old_lower = self.old.lower()

            for col in list(tree.find_all(exp.Column)):
                # 名称按不区分大小写比较
                if (col.name or '').lower() != old_lower:
                    continue
                prefix = col.table  # 可能是 None / 别名 / 基础表（含 schema 前缀）
                should_rename = False
                if prefix:
                    pref = prefix.split('.')[-1].lower()
                    mapped = alias_to_base.get(pref, pref)
                    if mapped == target_base:
                        should_rename = True
                else:
                    # 无前缀：仅当目标表在 FROM 中唯一出现时才替换
                    if target_unique and base_tables_count.get(target_base, 0) >= 1:
                        should_rename = True

                if should_rename:
                    col.set("this", exp.to_identifier(self.new))

            return tree.sql(dialect='mysql')
        except Exception:
            # 兜底：仅替换“无前缀”的旧列名（避免误动 a.old），大小写不敏感
            pattern = rf"(?<!\.)\b{re.escape(self.old)}\b"
            return re.sub(pattern, self.new, sql, flags=re.IGNORECASE)

    
