try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  # pragma: no cover
    from base import SMO, MySQLConstraintHelper

import re
import sqlglot
from sqlglot import expressions as exp


class HorizontalSplit(SMO):
    """
    表水平拆分：按谓词将一张表拆成多张分表。
    - predicates: [(new_table, where_clause_sql), ...]
    
    apply_to_schema:
      CREATE TABLE new_i AS SELECT * FROM old WHERE pred_i;
    apply_to_sql:
      将 FROM old 改为 (SELECT * FROM union all of new_i) 以保持语义。
    """

    def __init__(self, table: str, predicates: list[tuple[str, str]]):
        self.table = table
        self.predicates = predicates

    def apply_to_schema(self, db=None):
        """
        水平拆分并复制原表约束到每个分表：
        - 为每个分表 CTAS 创建数据；
        - 使用约束助手迁移主键/唯一/出站外键；
        - 复制默认值和自增设置；
        - 入站外键保持不变（仍指向旧表）。
        """
        stmts = []
        old = self.table
        helper = MySQLConstraintHelper(db) if db is not None else None
        constraints = helper.fetch_constraints(old) if helper else {'columns': []}

        def rebuild_column_props(new_tbl: str):
            colmeta = constraints.get('columns') or []
            def _lit(v: str):
                if v is None or v == 'NULL':
                    return 'NULL'
                try:
                    float(v)
                    return v
                except Exception:
                    pass
                v2 = str(v).replace("'", "''")
                return f"'{v2}'"
            for cm in colmeta:
                col = cm['COLUMN_NAME']
                default = cm.get('COLUMN_DEFAULT')
                extra = (cm.get('EXTRA') or '').lower()
                nullable = cm.get('IS_NULLABLE', 'YES')
                ctype = cm.get('COLUMN_TYPE') or 'varchar(255)'
                if default is not None:
                    stmts.append(f"ALTER TABLE `{new_tbl}` ALTER COLUMN `{col}` SET DEFAULT {_lit(str(default))}")
                if 'auto_increment' in extra:
                    stmts.append(
                        f"ALTER TABLE `{new_tbl}` MODIFY COLUMN `{col}` {ctype} "
                        f"{'NOT NULL' if nullable=='NO' else 'NULL'} AUTO_INCREMENT"
                    )

        stmts.append('SET FOREIGN_KEY_CHECKS=0')
        for new_tbl, where in self.predicates:
            stmts.append(
                f"CREATE TABLE `{new_tbl}` AS SELECT * FROM `{old}` WHERE {where};"
            )
            # 复制主键/唯一/出站外键
            if helper:
                include_cols = [c['COLUMN_NAME'] for c in (constraints.get('columns') or [])]
                add_stmts = helper.build_add_constraints_for_table(new_tbl, constraints, include_cols, rename_map=None)
                stmts.extend(add_stmts)
            # 复制默认值与自增
            rebuild_column_props(new_tbl)
        stmts.append('SET FOREIGN_KEY_CHECKS=1')

        if db is not None and hasattr(db, "execute_statement"):
            ok = True
            for s in stmts:
                ok = ok and db.execute_statement(s)
            return ok
        return "\n".join(stmts)

    def apply_to_sql(self, sql: str) -> str:
        """
        只读 SQL 改写：
        - 如果 WHERE 条件能够唯一命中某个分表的谓词，则将该表替换为该分表；
        - 否则，将该表替换为 (SELECT * FROM t1 UNION ALL SELECT * FROM t2 ...) 的子查询并保留别名。
        """

        def canonical_col(s: str) -> str:
            s = s.strip().strip('`')
            return s.split('.')[-1].lower()

        def canonical_val(s: str) -> str:
            s = s.strip()
            if s.startswith('`') and s.endswith('`'):
                s = s[1:-1]
            if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
                s = s[1:-1]
            return s

        def extract_eq_set(where_expr: exp.Expression | None) -> set[tuple[str, str]]:
            """提取等值条件集合 {(列, 值)}，仅处理 AND/EQ 的简单情况。"""
            res: set[tuple[str, str]] = set()
            if where_expr is None:
                return res
            def walk(node: exp.Expression):
                if isinstance(node, exp.And):
                    walk(node.left)
                    walk(node.right)
                    return
                if isinstance(node, exp.EQ):
                    lsql = node.left.sql(dialect='mysql')
                    rsql = node.right.sql(dialect='mysql')
                    # 识别哪边是列
                    if isinstance(node.left, exp.Column):
                        col = canonical_col(lsql)
                        val = canonical_val(rsql)
                        res.add((col, val))
                    elif isinstance(node.right, exp.Column):
                        col = canonical_col(rsql)
                        val = canonical_val(lsql)
                        res.add((col, val))
                    return
                # 其他情况忽略
            walk(where_expr)
            return res

        def predicate_eq_set(predicate_sql: str) -> set[tuple[str, str]]:
            try:
                ptree = sqlglot.parse_one(f"SELECT 1 WHERE {predicate_sql}")
                return extract_eq_set(ptree.args.get('where').this if ptree.args.get('where') else None)
            except Exception:
                # 兜底：简单解析 a=1 这类
                m = re.findall(r"([\w`.]+)\s*=\s*([^\s]+)", predicate_sql)
                return {(canonical_col(a), canonical_val(b)) for a, b in m}

        # 解析原 SQL
        try:
            tree = sqlglot.parse_one(sql)
        except Exception:
            # 解析失败，退回到原先的 UNION ALL 替换策略
            union = " UNION ALL ".join([f"SELECT * FROM {t}" for t, _ in self.predicates])
            replacement = f"FROM ( {union} ) AS {self.table}"
            pattern = re.compile(r"\bFROM\s+" + re.escape(self.table) + r"\b", re.IGNORECASE)
            return pattern.sub(replacement, sql)

        # where 等值条件集合
        where_node = tree.args.get('where')
        where_eqs = extract_eq_set(where_node.this if where_node else None)

        # 计算匹配的分表
        matched: list[str] = []
        part_cond_cache: dict[str, set[tuple[str, str]]] = {}
        for t, pred in self.predicates:
            eqs = predicate_eq_set(pred)
            part_cond_cache[t] = eqs
            if eqs and eqs.issubset(where_eqs):
                matched.append(t)

        # 替换目标表节点
        targets = list(tree.find_all(exp.Table))
        for tbl in targets:
            name = tbl.name
            base = name.split('.')[-1] if name else ''
            if base.lower() != self.table.lower():
                continue

            alias = tbl.args.get('alias')  # 可能为 None
            alias_sql = None
            if alias is not None:
                alias_sql = alias.sql(dialect='mysql').split()[-1]
            # 1) 唯一命中一个分表：直接替换表名
            if len(matched) == 1:
                new_table = exp.Table(this=exp.Identifier(this=matched[0], quoted=False))
                new_node: exp.Expression
                if alias is not None:
                    new_node = exp.Alias(this=new_table, alias=alias)
                else:
                    new_node = new_table
                tbl.replace(new_node)
            else:
                # 2) 否则替换为 UNION ALL 子查询，保留别名
                # 如果没有唯一匹配但存在多个匹配，可以只 union 匹配到的；
                # 这里按需求使用所有子表。
                union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t, _ in self.predicates])
                alias_name = alias_sql or self.table
                sub_sql = f"(SELECT * FROM {self.predicates[0][0]}"  # will be replaced next line
                sub_sql = f"( {union_sql} ) AS {alias_name}"
                sub_node = sqlglot.parse_one(sub_sql)
                tbl.replace(sub_node)

        return tree.sql()

    def apply_to_data(self, row):
        return row
