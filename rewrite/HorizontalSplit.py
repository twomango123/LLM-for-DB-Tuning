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
    - is_retained: 是否保留原表

    当 is_retained=False（默认）：
      apply_to_schema:
        为每个分片创建实体表：CREATE TABLE new_i AS SELECT * FROM old WHERE pred_i;
        复制主键/唯一/出站外键/默认/自增（尽力）。
      apply_to_sql:
        根据 WHERE 唯一命中替换为对应分表；否则替换为所有分表的 UNION ALL 子查询。

    当 is_retained=True（保留原表）：
      apply_to_schema:
        为每个分片创建视图：CREATE OR REPLACE VIEW new_i AS SELECT * FROM old WHERE pred_i;
        原表不变，不复制约束。
      apply_to_sql:
        根据 WHERE 唯一命中替换为对应视图；无法唯一命中时保持原表不变。
    """

    def __init__(self, table: str, predicates: list[tuple[str, str]], is_retained: bool = False):
        self.table = table
        self.predicates = predicates
        self.is_retained = is_retained

    def apply_to_schema(self, db=None):
        """
        水平拆分：根据 is_retained 选择创建实体表或视图。
        """
        old = self.table
        # 保留原表：创建视图
        if getattr(self, 'is_retained', False):
            stmts = []
            for new_tbl, where in self.predicates:
                stmts.append(
                    f"CREATE OR REPLACE VIEW `{new_tbl}` AS SELECT * FROM `{old}` WHERE {where};"
                )
            if db is not None and hasattr(db, "execute_statement"):
                ok = True
                for s in stmts:
                    ok = ok and db.execute_statement(s)
                return ok
            return "\n".join(stmts)

        # 不保留原表：创建实体表并复制约束
        stmts = []
        helper = MySQLConstraintHelper(db) if db is not None else None
        constraints = helper.fetch_constraints(old) if helper else {'columns': []}

        # 零日期防护：为 DATE/DATETIME/TIMESTAMP 列构造安全投影
        def _dtype_map(cons):
            mp = {}
            for cm in (cons.get('columns') or []):
                nm = cm.get('COLUMN_NAME')
                dt = (cm.get('DATA_TYPE') or '').lower()
                if nm:
                    mp[nm] = dt
            return mp
        dtype_map = _dtype_map(constraints)

        def _safe_dt_expr(alias: str, col: str, dtype: str) -> str:
            q = f"{alias}.`{col}`"
            d = (dtype or '').lower()
            if d == 'date':
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00') AS DATE)"
            if d in ('datetime','timestamp'):
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00 00:00:00') AS DATETIME)"
            return q

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
            # 覆盖重建：先删后建
            stmts.append(f"DROP TABLE IF EXISTS `{new_tbl}`;")
            if dtype_map:
                cols = [cm['COLUMN_NAME'] for cm in (constraints.get('columns') or [])]
                proj = ", ".join(
                    [f"{_safe_dt_expr('t', c, dtype_map.get(c,''))} AS `{c}`" for c in cols]
                )
                stmts.append(
                    f"CREATE TABLE `{new_tbl}` AS SELECT {proj} FROM `{old}` t WHERE {where};"
                )
            else:
                stmts.append(f"CREATE TABLE `{new_tbl}` AS SELECT * FROM `{old}` WHERE {where};")
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
        - 否则：
            * 当 is_retained=False 时，替换为 (SELECT * FROM t1 UNION ALL SELECT * FROM t2 ...) 子查询并保留别名；
            * 当 is_retained=True 时，保持原表不变。
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
            # 解析失败，退回到原先的 UNION ALL 替换策略，仅处理只读 SELECT 的 FROM 段
            head = sql.strip().lower()
            if not head.startswith('select'):
                return sql
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

        def _is_dml_target(tbl: exp.Table) -> bool:
            # 跳过 DML 语句的目标表（INSERT/UPDATE/DELETE 的主表），仅改写其内部子查询
            p = tbl.parent
            while p is not None:
                if isinstance(p, exp.Insert) and p.this is tbl:
                    return True
                if isinstance(p, exp.Update) and p.this is tbl:
                    return True
                if isinstance(p, exp.Delete) and p.this is tbl:
                    return True
                p = p.parent
            return False
        for tbl in targets:
            name = tbl.name
            base = name.split('.')[-1] if name else ''
            if base.lower() != self.table.lower():
                continue
            # 保留 DML 的目标表不改写，避免生成 "INSERT INTO (subquery)" 等非法语句
            if _is_dml_target(tbl):
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
                # 2) 无唯一匹配
                if not getattr(self, 'is_retained', False):
                    # 不保留原表 → 使用 UNION ALL 子查询替换（仅限只读场景，DML 目标表已在上方跳过）
                    union_sql = " UNION ALL ".join([f"SELECT * FROM {t}" for t, _ in self.predicates])
                    alias_name = alias_sql or self.table
                    sub_sql = f"( {union_sql} ) AS {alias_name}"
                    sub_node = sqlglot.parse_one(sub_sql)
                    tbl.replace(sub_node)
                else:
                    # 保留原表 → 不替换
                    pass

        return tree.sql()

    def apply_to_data(self, row):
        return row

    def apply_to_write_sql(self, sql: str) -> str:
        """
        写入 SQL 改写：当 is_retained=True 时，分片以视图形式存在，不需要改写写入语句（写入仍指向基表）。
        当 is_retained=False（实体分表），对于 INSERT/UPDATE 到旧表：
        - INSERT：如果能用等值谓词唯一命中某分片（基于插入列和值），则把目标表替换到该分片；否则保留原表（交由上游路由层处理）。
        - UPDATE：若 WHERE 等值能唯一命中某分片，则把目标表替换到该分片；否则保留原表。
        保守策略：仅处理“唯一命中”的情况，避免错误路由。
        """
        if getattr(self, 'is_retained', False):
            return sql
        try:
            tree = sqlglot.parse_one(sql, read='mysql')
        except Exception:
            return sql

        base = self.table.lower()

        def eq_set_from_pairs(pairs: list[tuple[str, str]]) -> set[tuple[str, str]]:
            return {(a.lower(), b) for a, b in pairs}

        def parse_pred_eqs(predicate_sql: str) -> set[tuple[str, str]]:
            try:
                pt = sqlglot.parse_one(f"SELECT 1 WHERE {predicate_sql}")
                where = pt.args.get('where')
                if not where:
                    return set()
                res = set()
                for n in where.this.find_all(exp.EQ):
                    l, r = n.left, n.right
                    if isinstance(l, exp.Column) and isinstance(r, exp.Literal):
                        res.add((l.name.lower(), r.this))
                    elif isinstance(r, exp.Column) and isinstance(l, exp.Literal):
                        res.add((r.name.lower(), l.this))
                return res
            except Exception:
                return set()

        # INSERT
        if isinstance(tree, exp.Insert):
            tbl = tree.this
            t = (tbl.name or '').split('.')[-1].lower() if isinstance(tbl, exp.Table) else ''
            if t != base:
                return sql
            cols = [c.name for c in (tree.args.get('columns') or [])]
            vals = tree.args.get('expression')
            # 仅处理 INSERT ... VALUES (...) 且所有行一致的情形
            if isinstance(vals, exp.Values) and cols:
                # 简化：仅取第一行构造等值集合
                row = list(vals.expressions)[0]
                lit_pairs = []
                for c, v in zip(cols, row.expressions):
                    if isinstance(v, exp.Literal):
                        lit_pairs.append((c, v.this))
                eqs = eq_set_from_pairs(lit_pairs)
                # 找唯一命中
                matched = None
                for tname, pred in self.predicates:
                    pe = parse_pred_eqs(pred)
                    if pe and pe.issubset(eqs):
                        matched = tname; break
                if matched:
                    new_tbl = exp.Table(this=exp.Identifier(this=matched, quoted=False))
                    tree.set('this', new_tbl)
                    return tree.sql(dialect='mysql')
            return sql

        # UPDATE
        if isinstance(tree, exp.Update):
            tbl = tree.this
            t = (tbl.name or '').split('.')[-1].lower() if isinstance(tbl, exp.Table) else ''
            if t != base:
                return sql
            where = tree.args.get('where')
            if where is not None and where.this is not None:
                res = set()
                for n in where.this.find_all(exp.EQ):
                    l, r = n.left, n.right
                    if isinstance(l, exp.Column) and isinstance(r, exp.Literal):
                        res.add((l.name.lower(), r.this))
                    elif isinstance(r, exp.Column) and isinstance(l, exp.Literal):
                        res.add((r.name.lower(), l.this))
                matched = None
                for tname, pred in self.predicates:
                    pe = parse_pred_eqs(pred)
                    if pe and pe.issubset(res):
                        matched = tname; break
                if matched:
                    tree.set('this', exp.Table(this=exp.Identifier(this=matched, quoted=False)))
                    return tree.sql(dialect='mysql')
            return sql

        return sql

    # ---------- performance eval ----------
    def evaluate_on_plan(self, plan_text: str,
                         meta_path: str | None = 'output_dir/meta.json',
                         sample_union_cost: float | None = None,
                         sample_union_rows: float | None = None) -> dict:
        """
        评估水平拆分对计划的影响：
        - is_retained=False：
          * 若查询 WHERE 与某个分片谓词一致，则把原表基数替换为该子表基数（rows_factor），成本随之缩放；
          * 若查询未命中任一分片谓词，则增加 UNION ALL 聚合代价：使用 sample_union_cost/rows 按实际子表基数线性缩放。
        - is_retained=True：
          * 命中某分片 → rows 基数替换；否则成本不变。
        """
        try:
            # Use unified cost model wrappers
            from rewrite.cost_model import (
                load_meta, table_rows, CostModel
            )
            from performance_eval.plan import parse_plan, compute_total_cost
        except Exception:
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': 'performance_eval 不可用，跳过评估'
            }

        meta = load_meta(meta_path)
        base = self.table
        nodes = parse_plan(plan_text)
        if not any(base in (n.tables or []) for n in nodes):
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': '计划未引用该表，跳过'
            }

        # 规则更新：当筛选条件与拆分条件一致时，选择率改变。
        # 我们优先尝试使用“过滤剪枝(type3)”按谓词文本识别并消除匹配的 Filter，
        # 同时将扫描输入行数缩小到该 Filter 的输出（即按选择率缩放）。
        cm = CostModel()
        patterns = [where for (_t, where) in self.predicates]
        try:
            pruned = cm.prune_filters(plan_text, patterns=patterns, regex=False, combine='product', cols_factor=1.0)
            # 若找到了可匹配的过滤条件，则直接返回剪枝后的计划与成本
            if pruned and pruned.get('new_total_cost') is not None and pruned.get('new_total_cost') != pruned.get('original_total_cost'):
                pruned['note'] = '命中分片谓词：消除相应Filter，并按选择率重分配至扫描（选择率已变化）'
                # 标注不能直接缩放得到的项
                try:
                    ns_terms = []
                    for n in parse_plan(plan_text):
                        if 'join' in n.type:
                            ns_terms.append({'op': n.type, 'need': ['S(连接选择率)']})
                        if n.type in ('group_temp', 'group_agg'):
                            ns_terms.append({'op': n.type, 'need': ['G(分组数)']})
                        if n.type == 'index_scan':
                            ns_terms.append({'op': n.type, 'need': ['Sx(选择率)']})
                    if ns_terms:
                        pruned['non_scalable'] = ns_terms
                except Exception:
                    pass
                return pruned
        except Exception:
            pass

        # 若未识别到匹配的Filter，则回退到“命中分片→按基数缩放”的近似处理
        # 判断是否命中某个分片（基于元数据行数启发式）
        part_rows: list[tuple[str, float]] = []
        for t, _w in self.predicates:
            r = table_rows(meta, t)
            if r is not None:
                part_rows.append((t, float(r)))
        base_rows = table_rows(meta, base) or 0.0

        # 粗略规则：若存在一个分片行数显著小于基表（< 0.9*base），认为命中该分片
        hit = None
        for t, r in part_rows:
            if base_rows and r <= 0.9 * base_rows:
                hit = (t, r)
                break

        if hit:
            # 命中分片：rows_factor = r_part / r_base （选择率变化体现在扫描侧）
            t_hit, r_hit = hit
            rf = (r_hit or 0.0) / max(base_rows, 1e-9)
            res = cm.apply_type1(plan_text, target_table=base, rows_factor=rf, cols_factor=1.0, filter_factor=1.0)
            res['note'] = f"命中分片 {t_hit}：按基数缩放（未显式匹配到Filter文本）"
            # 列出不能直接通过缩放获得的项（按计划包含的算子推断）
            try:
                ns_terms = []
                for n in parse_plan(plan_text):
                    if 'join' in n.type:
                        ns_terms.append({'op': n.type, 'need': ['S(连接选择率)']})
                    if n.type in ('group_temp', 'group_agg'):
                        ns_terms.append({'op': n.type, 'need': ['G(分组数)']})
                    if n.type == 'index_scan':
                        ns_terms.append({'op': n.type, 'need': ['Sx(选择率)']})
                res['non_scalable'] = ns_terms
            except Exception:
                pass
            return res

        # 未命中：
        if not getattr(self, 'is_retained', False):
            # 不保留：需要 UNION ALL 聚合两个（或多个）子表
            total_rows = sum(r for _, r in part_rows) if part_rows else 0.0
            before = compute_total_cost(nodes)
            add = 0.0
            if sample_union_cost is not None:
                # 若 meta 中拿不到子表基数，回退用样本行数
                from rewrite.cost_model import union_cost_linear
                eff_total = total_rows if total_rows > 0 else (sample_union_rows or 0.0)
                add = union_cost_linear(sample_union_cost, (sample_union_rows or eff_total or 1.0), eff_total/2.0, eff_total/2.0)
            # 不改树结构，仅把总代价加上“合并代价”作为估计
            return {
                'new_plan_text': plan_text,
                'original_total_cost': before,
                'new_total_cost': before + add,
                'delta': add,
                'note': f'未命中分片，增加 UNION ALL 代价≈{add:.3g}',
                'non_scalable': [{'op': 'UNION ALL', 'need': ['样本合并代价/行数用于线性缩放']}]
            }

        # 保留：不变
        return {
            'new_plan_text': plan_text,
            'original_total_cost': None,
            'new_total_cost': None,
            'delta': 0.0,
            'note': '保留原表且未命中分片：成本不变'
        }
