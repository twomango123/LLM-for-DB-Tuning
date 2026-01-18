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
            # 使用 'postgres' 解析以便支持 WITH/CTE 与派生表节点；输出仍用 MySQL 方言
            tree = parse_one(sql, read='postgres')

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

            # --------- 阶段1：记录派生表/CTE 投影列在改名前的可见列名 ---------
            def _alias_of_subquery(sq) -> str | None:
                a = getattr(sq, 'alias', None)
                if a is None:
                    return None
                # 在本 sqlglot 版本中 Subquery.alias 多为 str
                if isinstance(a, str):
                    return a
                name = getattr(a, 'name', None)
                return name

            def _proj_label(expr) -> str | None:
                # 优先使用别名；否则当表达式是 Column 时使用列名
                alias_node = expr.args.get('alias') if hasattr(expr, 'args') else None
                if alias_node is not None:
                    # 兼容 Identifier/字符串
                    if hasattr(alias_node, 'name') and alias_node.name:
                        return alias_node.name
                    if isinstance(alias_node, str):
                        return alias_node
                col = expr.find(exp.Column)
                if isinstance(expr, exp.Column) and col is not None:
                    return col.name
                return None

            # 子查询/派生表
            derived_before: dict[str, dict[int, str | None]] = {}
            for sq in list(tree.find_all(exp.Subquery)):
                alias = _alias_of_subquery(sq)
                if not alias:
                    continue
                sel = sq.this
                if isinstance(sel, exp.Select):
                    labels = {}
                    for i, proj in enumerate(list(sel.expressions)):
                        labels[i] = _proj_label(proj)
                    derived_before[alias] = labels

            # CTE（WITH 子句）
            cte_before: dict[str, dict[int, str | None]] = {}
            for cte in list(tree.find_all(exp.CTE)):
                alias = getattr(cte, 'alias', None)
                if isinstance(alias, str):
                    name = alias
                else:
                    name = getattr(alias, 'name', None)
                if not name:
                    continue
                sel = cte.this
                if isinstance(sel, exp.Select):
                    labels = {}
                    for i, proj in enumerate(list(sel.expressions)):
                        labels[i] = _proj_label(proj)
                    cte_before[name] = labels

            target_base = self.table.split('.')[-1].lower()
            target_unique = base_tables_count.get(target_base, 0) == 1

            old_lower = self.old.lower()

            # --------- 阶段2：改写所有层级中的列引用（含子查询/CTE 内部） ---------
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

            # --------- 阶段3：对子查询/CTE 的“外显列名”进行同步与外层引用改写 ---------
            def _refresh_mapping(before_map: dict[str, dict[int, str | None]], is_cte=False) -> dict[str, dict[str, str]]:
                result: dict[str, dict[str, str]] = {}
                for alias, idx2name in before_map.items():
                    after: dict[int, str | None] = {}
                    # 获取对应的 Select 节点
                    # 子查询通过 alias 查找；CTE 则遍历 CTE 列表
                    if not is_cte:
                        # Subquery
                        select_nodes = [sq.this for sq in tree.find_all(exp.Subquery) if _alias_of_subquery(sq) == alias]
                    else:
                        select_nodes = [cte.this for cte in tree.find_all(exp.CTE)
                                        if (getattr(cte, 'alias', None) == alias) or
                                           (hasattr(getattr(cte, 'alias', None), 'name') and getattr(cte, 'alias').name == alias)]
                    if not select_nodes:
                        continue
                    sel = select_nodes[0]
                    if not isinstance(sel, exp.Select):
                        continue
                    # 1) 若投影有显式别名且等于旧名，则把别名同步改为新名
                    for i, proj in enumerate(list(sel.expressions)):
                        alias_node = proj.args.get('alias') if hasattr(proj, 'args') else None
                        if alias_node is not None:
                            # 获取当前别名
                            name_now = getattr(alias_node, 'name', None) if hasattr(alias_node, 'name') else (
                                alias_node if isinstance(alias_node, str) else None
                            )
                            if name_now and name_now.lower() == old_lower:
                                proj.set('alias', exp.to_identifier(self.new))
                    # 2) 重新计算改名后的外显列名
                    for i, proj in enumerate(list(sel.expressions)):
                        after[i] = _proj_label(proj)
                    # 3) 生成 old->new 的映射
                    mapping: dict[str, str] = {}
                    for i, old_label in idx2name.items():
                        new_label = after.get(i)
                        if old_label and new_label and old_label.lower() != new_label.lower():
                            mapping[old_label] = new_label
                    if mapping:
                        result[alias] = mapping
                return result

            derived_renames = _refresh_mapping(derived_before, is_cte=False)
            cte_renames = _refresh_mapping(cte_before, is_cte=True)
            alias_to_renames: dict[str, dict[str, str]] = {}
            alias_to_renames.update(derived_renames)
            alias_to_renames.update(cte_renames)

            if alias_to_renames:
                for col in list(tree.find_all(exp.Column)):
                    alias = col.table
                    if not alias:
                        continue
                    mapping = alias_to_renames.get(alias)
                    if not mapping:
                        continue
                    new_name = None
                    # 不区分大小写匹配 key
                    for k, v in mapping.items():
                        if col.name.lower() == k.lower():
                            new_name = v
                            break
                    if new_name:
                        col.set('this', exp.to_identifier(new_name))

            return tree.sql(dialect='mysql')
        except Exception:
            # 兜底：仅替换“无前缀”的旧列名（避免误动 a.old），大小写不敏感
            pattern = rf"(?<!\.)\b{re.escape(self.old)}\b"
            return re.sub(pattern, self.new, sql, flags=re.IGNORECASE)

    
