try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  # pragma: no cover
    from base import SMO, MySQLConstraintHelper

import re


class HorizontalMerge(SMO):
    """
    表水平合并：将多张同结构分表并回一张表或视图。
    - sources: List[str] 源表列表
    - new_table: 目标表/视图名
    - is_retained: 是否保留源表（True=保留，创建视图；False=不保留，创建实体表）

    当 is_retained=False（默认）：
      apply_to_schema: CREATE TABLE new AS SELECT * FROM s1 UNION ALL SELECT * FROM s2 ...，并按规则迁移约束/默认/自增。
      apply_to_sql: 将 FROM 任一 s_i 替换为 new_table；若包含多个 s_i，则保留一个并移除其它。

    当 is_retained=True：
      apply_to_schema: CREATE OR REPLACE VIEW new AS SELECT * FROM s1 UNION ALL SELECT * FROM s2 ...（不迁移约束）。
      apply_to_sql: 将 FROM 中所有 s_i 替换为该视图名（效果与表相同，仍然只保留一个以避免重复）。
    """

    def __init__(self, sources: list[str], new_table: str, is_retained: bool = False):
        self.sources = sources
        self.new_table = new_table
        self.is_retained = is_retained

    def apply_to_schema(self, db=None):
        """
        根据 is_retained 创建视图或实体表；实体表场景下按规则重建约束（MySQL 专用）。
        """
        # 为 DATE/DATETIME/TIMESTAMP 列在 UNION 路径上统一做零日期防护
        helper = MySQLConstraintHelper(db) if db is not None else None
        cons = helper.fetch_constraints(self.sources[0]) if (helper and self.sources) else {'columns': []}
        def _dtype_map(cons):
            mp = {}
            for cm in (cons.get('columns') or []):
                nm = cm.get('COLUMN_NAME')
                dt = (cm.get('DATA_TYPE') or '').lower()
                if nm:
                    mp[nm] = dt
            return mp
        dtype = _dtype_map(cons)
        def _safe(alias: str, col: str) -> str:
            q = f"{alias}.`{col}`"
            d = (dtype.get(col,'') or '').lower()
            if d == 'date':
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00') AS DATE)"
            if d in ('datetime','timestamp'):
                return f"CAST(NULLIF(CONCAT({q}), '0000-00-00 00:00:00') AS DATETIME)"
            return q
        if dtype:
            cols = [cm['COLUMN_NAME'] for cm in (cons.get('columns') or [])]
            select_star = lambda s: ", ".join([f"{_safe('t', c)} AS `{c}`" for c in cols])
            union = " UNION ALL ".join([f"SELECT {select_star(s)} FROM `{s}` t" for s in self.sources])
        else:
            union = " UNION ALL ".join([f"SELECT * FROM `{s}`" for s in self.sources])

        # 保留源表：创建视图
        if getattr(self, 'is_retained', False):
            create_view = f"CREATE OR REPLACE VIEW `{self.new_table}` AS {union};"
            if db is not None and hasattr(db, 'execute_statement'):
                return db.execute_statement(create_view)
            return create_view

        # 不保留：创建实体表并迁移约束
        # 幂等覆盖：先删再建
        drop = f"DROP TABLE IF EXISTS `{self.new_table}`;"
        create = f"CREATE TABLE `{self.new_table}` AS {union};"
        stmts = ['SET FOREIGN_KEY_CHECKS=0', drop, create]

        helper = MySQLConstraintHelper(db) if db is not None else None
        if helper and self.sources:
            # 采集所有源表的约束/列元数据
            cons_all = [helper.fetch_constraints(s) for s in self.sources]

            # 主键/外键/检查以第一个源表为准（结构相同的前提）
            cons0 = cons_all[0]
            include_cols = [c['COLUMN_NAME'] for c in (cons0.get('columns') or [])]
            add_stmts = helper.build_add_constraints_for_table(
                self.new_table, cons0, include_cols, rename_map=None
            )
            # 过滤 UNIQUE（不重建）
            add_stmts = [s for s in add_stmts if ' UNIQUE ' not in s.upper()]
            stmts.extend(add_stmts)

            # 计算各列的默认值一致性 & 自增冲突
            # 构造列 -> [每个源表默认值]
            from collections import defaultdict
            defaults_map = defaultdict(list)
            ai_count = defaultdict(int)  # 列 -> 有自增的源表数量
            col_type_map = {}  # 优先取第一个源表类型
            col_nullable_map = {}
            for idx, cons in enumerate(cons_all):
                for cm in cons.get('columns') or []:
                    col = cm['COLUMN_NAME']
                    if idx == 0:
                        col_type_map[col] = cm.get('COLUMN_TYPE') or 'varchar(255)'
                        col_nullable_map[col] = cm.get('IS_NULLABLE', 'YES')
                    defaults_map[col].append(cm.get('COLUMN_DEFAULT'))
                    extra = (cm.get('EXTRA') or '').lower()
                    if 'auto_increment' in extra:
                        ai_count[col] += 1

            def _lit(v):
                if v is None or v == 'NULL':
                    return 'NULL'
                try:
                    float(v)
                    return str(v)
                except Exception:
                    pass
                return "'" + str(v).replace("'", "''") + "'"

            # 默认值：仅当所有源表默认完全一致时复制
            for col, lst in defaults_map.items():
                if len(lst) == len(self.sources) and all(x == lst[0] for x in lst):
                    if lst[0] is not None:
                        stmts.append(
                            f"ALTER TABLE `{self.new_table}` ALTER COLUMN `{col}` SET DEFAULT {_lit(lst[0])}"
                        )

            # 自增：仅当该列恰有一个源表带有 AUTO_INCREMENT 时复制
            for col, cnt in ai_count.items():
                if cnt == 1:
                    ctype = col_type_map.get(col, 'varchar(255)')
                    nullable = col_nullable_map.get(col, 'YES')
                    stmts.append(
                        f"ALTER TABLE `{self.new_table}` MODIFY COLUMN `{col}` {ctype} "
                        f"{'NOT NULL' if nullable=='NO' else 'NULL'} AUTO_INCREMENT"
                    )

        stmts.append('SET FOREIGN_KEY_CHECKS=1')

        if db is not None and hasattr(db, 'execute_statement'):
            ok = True
            for s in stmts:
                ok = ok and db.execute_statement(s)
            return ok
        return "\n".join(stmts)

    def apply_to_sql(self, sql: str) -> str:
        pattern = re.compile(r"\bFROM\s+([^;]+?)(?=\s+WHERE|\s+GROUP|\s+ORDER|\s+UNION|\)|$)", re.IGNORECASE | re.DOTALL)

        def repl(match):
            tables_part = match.group(1)
            tables = [t.strip() for t in tables_part.split(',')]
            new_tables = []
            replaced = False
            for t in tables:
                base_table = t.split()[0]
                if base_table in self.sources:
                    if not replaced:
                        new_tables.append(t.replace(base_table, self.new_table, 1))
                        replaced = True
                    # 其他源表去掉
                else:
                    new_tables.append(t)
            return f"FROM " + ', '.join(new_tables)

        return pattern.sub(repl, sql)

    def apply_to_data(self, row):
        return row

    def apply_to_write_sql(self, sql: str) -> str:
        """
        写入 SQL 改写（保守）：
        - 若 INSERT 的目标就是 new_table 且为 INSERT INTO new_table SELECT ...，
          则允许对 SELECT 子句做只读 apply_to_sql 改写，将对源表的读取替换为 new_table（避免再次 UNION）。
        - 针对写入源表的 INSERT/UPDATE 不做“合并到 new_table”的自动改写，避免双写/路由错误。
        其它情况返回原 SQL。
        """
        try:
            import sqlglot
            from sqlglot import expressions as exp
            tree = sqlglot.parse_one(sql, read='mysql')
        except Exception:
            return sql
        if isinstance(tree, exp.Insert):
            dst = tree.this
            sel = tree.args.get('expression')
            if isinstance(dst, exp.Table) and isinstance(sel, (exp.Select, exp.Subquery)):
                dst_name = (dst.name or '').split('.')[-1]
                if dst_name == self.new_table:
                    new_sql = self.apply_to_sql(sel.sql(dialect='mysql'))
                    try:
                        tree.set('expression', sqlglot.parse_one(new_sql, read='mysql'))
                        return tree.sql(dialect='mysql')
                    except Exception:
                        return sql
        return sql

    # ---------- performance eval ----------
    def evaluate_on_plan(self, plan_text: str,
                         meta_path: str | None = 'output_dir/meta.json',
                         sample_union_cost: float | None = None,
                         sample_union_rows: float | None = None,
                         predicates: list[str] | None = None) -> dict:
        """
        评估水平合并（多表并回一表/视图）对计划的影响：
        - 若计划引用任意一个源表：
          * 不保留源表（创建实体表）：将对源表的基数/宽度替换为 new_table；
          * 保留源表（视图）：若查询涉及多个源表，去除 UNION ALL 合并代价；否则不变。
        这里采用简化近似：
          - 替换基表 → 类型1 rows_factor = new_rows/old_rows，cols_factor = width_new/width_old。
          - 移除 UNION 代价 → 用 sample_union_cost 作为上界，直接从总成本减去（若提供）。
        """
        try:
            from rewrite.cost_model import (
                load_meta, table_rows, avg_row_width, CostModel
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
        nodes = parse_plan(plan_text)
        touched = [s for s in self.sources if any(s in (n.tables or []) for n in nodes)]
        if not touched:
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': '计划未引用源表，跳过'
            }

        new_rows = table_rows(meta, self.new_table) or 0.0
        new_w = (avg_row_width(meta, self.new_table) or 1.0)

        # 若不保留源表：对首个命中的源表按 new_table 缩放
        if not getattr(self, 'is_retained', False):
            s = touched[0]
            old_rows = table_rows(meta, s) or 0.0
            old_w = (avg_row_width(meta, s) or 1.0)
            rf = (new_rows or 0.0) / max(old_rows, 1e-9) if old_rows else 1.0
            cf = (new_w or 1.0) / max(old_w, 1e-9)
            cm = CostModel()
            res = cm.apply_type1(plan_text, target_table=s, rows_factor=rf, cols_factor=cf, filter_factor=1.0)
            res['note'] = f'不保留：用 {self.new_table} 替换 {s} 的基数/宽度'
            # 若提供了分片谓词（与筛选条件一致），在合并后按谓词重分配选择率（可能不同）
            if predicates:
                try:
                    pruned = cm.prune_filters(res['new_plan_text'], patterns=predicates, regex=False, combine='product', cols_factor=1.0)
                    if pruned and pruned.get('new_total_cost') is not None:
                        res['new_plan_text'] = pruned['new_plan_text']
                        # original_total_cost 仍以 res 的 original 为准
                        base = res.get('original_total_cost')
                        res['new_total_cost'] = pruned.get('new_total_cost')
                        if base is not None and res['new_total_cost'] is not None:
                            res['delta'] = res['new_total_cost'] - base
                        res['note'] += '；命中谓词：合并后选择率按谓词剪枝重新分配'
                except Exception:
                    pass
            # 不能直接缩放得到的项
            try:
                from rewrite.cost_model import parse_plan as _pp
                ns_terms = []
                for n in _pp(plan_text):
                    if 'join' in n.type:
                        ns_terms.append({'op': n.type, 'need': ['S(连接选择率)']})
                    if n.type in ('group_temp', 'group_agg'):
                        ns_terms.append({'op': n.type, 'need': ['G(分组数)']})
                    if n.type == 'index_scan':
                        ns_terms.append({'op': n.type, 'need': ['Sx(选择率)']})
                if ns_terms:
                    res['non_scalable'] = ns_terms
            except Exception:
                pass
            return res

        # 保留：若查询会访问多个源表，估计去除 UNION 聚合代价；之后若给出谓词，按谓词重分配选择率
        num_hit = len(touched)
        if num_hit >= 2 and sample_union_cost is not None:
            total_before = compute_total_cost(nodes)
            # 简化：直接减去提供的样本合并代价
            new_total = max(total_before - float(sample_union_cost), 0.0)
            out = {
                'new_plan_text': plan_text,
                'original_total_cost': total_before,
                'new_total_cost': new_total,
                'delta': new_total - total_before,
                'note': '保留：多源访问，去除 UNION 代价（近似）',
                'non_scalable': [{'op': 'UNION ALL', 'need': ['样本合并代价/行数用于线性缩放']}]
            }
            if predicates:
                try:
                    cm = CostModel()
                    pruned = cm.prune_filters(plan_text, patterns=predicates, regex=False, combine='product', cols_factor=1.0)
                    if pruned and pruned.get('new_total_cost') is not None:
                        out['new_plan_text'] = pruned['new_plan_text']
                        out['new_total_cost'] = max(pruned['new_total_cost'] - float(sample_union_cost), 0.0)
                        out['delta'] = out['new_total_cost'] - total_before
                        out['note'] += '；命中谓词：合并后选择率按谓词剪枝重新分配'
                except Exception:
                    pass
            return out

        return {
            'new_plan_text': plan_text,
            'original_total_cost': None,
            'new_total_cost': None,
            'delta': 0.0,
            'note': '保留且单源访问，成本不变'
        }
