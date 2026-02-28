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
        if db is None:
            # 若外部已完成外键校验，允许 dry-run 生成脚本
            if not getattr(self, "_fk_ok", False):
                raise ValueError("RedundantColumnAdd 需要数据库连接或预先完成外键校验")
        else:
            # join_keys 必须对应 source<->target 的外键关系，否则拒绝执行
            self._validate_fk_relation(db)

        on_clause = " AND ".join([f"t.`{tkey}` = s.`{skey}`" for (skey, tkey) in self.join_keys])

        # 精简版执行序列：不再尝试“若存在则先删除列”，由外部流程确保目标列不存在。
        # 这样可避免 PREPARE/EXECUTE 以及旧版本 MySQL 对 IF EXISTS 的兼容问题。
        # 零日期防护：为 t.* 与 s.`{sc}` 的日期/时间列添加 NULLIF 包装
        helper = MySQLConstraintHelper(db) if db is not None else None
        def _dtype_map(cons):
            mp = {}
            for cm in (cons.get('columns') or []):
                nm = cm.get('COLUMN_NAME')
                dt = (cm.get('DATA_TYPE') or '').lower()
                if nm:
                    mp[nm] = dt
            return mp
        def _safe(alias: str, col: str, dtype: str) -> str:
            q = f"{alias}.`{col}`"
            d = (dtype or '').lower()
            if d == 'date':
                return f"CAST(NULLIF(CONCAT({q}),'0000-00-00') AS DATE)"
            if d in ('datetime','timestamp'):
                return f"CAST(NULLIF(CONCAT({q}),'0000-00-00 00:00:00') AS DATETIME)"
            return q

        proj_t = "t.*"
        proj_s = f"s.`{sc}` AS `{nc}`"
        if helper is not None:
            cons_t = helper.fetch_constraints(t)
            cons_s = helper.fetch_constraints(s)
            dt_t = _dtype_map(cons_t)
            dt_s = _dtype_map(cons_s)
            cols_t = [cm['COLUMN_NAME'] for cm in (cons_t.get('columns') or [])]
            if cols_t:
                proj_t = ", ".join([f"{_safe('t', c, dt_t.get(c,''))} AS `{c}`" for c in cols_t])
            # 对源列单独防护
            proj_s = f"{_safe('s', sc, dt_s.get(sc,''))} AS `{nc}`"

        stmts = [
            'SET FOREIGN_KEY_CHECKS=0',
            f"DROP TABLE IF EXISTS `{t}__tmp_rca`;",
            f"CREATE TABLE `{t}__tmp_rca` AS SELECT {proj_t}, {proj_s} FROM `{t}` t LEFT JOIN `{s}` s ON {on_clause};",
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

    def _validate_fk_relation(self, db):
        """join_keys 必须来自 source/target 之间的外键关系（任意方向）。"""
        helper = MySQLConstraintHelper(db)
        cons_t = helper.fetch_constraints(self.target_table)
        cons_s = helper.fetch_constraints(self.source_table)

        def _norm_pairs(pairs):
            return {(str(a).lower(), str(b).lower()) for (a, b) in pairs}

        jk = _norm_pairs(self.join_keys)

        candidates = []

        # 方向1：target -> source 的外键
        for fk in cons_t.get('foreign_keys_outbound', []) or []:
            ref_table = (fk['cols'][0][1] if fk.get('cols') else '')
            if ref_table.lower() != self.source_table.lower():
                continue
            pairs = [(refc, child) for (child, _, refc) in fk['cols']]
            candidates.append(pairs)
            if _norm_pairs(pairs) == jk:
                return True

        # 方向2：source -> target 的外键
        for fk in cons_s.get('foreign_keys_outbound', []) or []:
            ref_table = (fk['cols'][0][1] if fk.get('cols') else '')
            if ref_table.lower() != self.target_table.lower():
                continue
            pairs = [(child, refc) for (child, _, refc) in fk['cols']]
            candidates.append(pairs)
            if _norm_pairs(pairs) == jk:
                return True

        cand_msg = "; ".join([str(c) for c in candidates]) if candidates else "无"
        raise ValueError(
            "RedundantColumnAdd join_keys 必须对应 source/target 之间的外键关系，未找到匹配外键。"
            f" join_keys={self.join_keys}; 可用外键映射={cand_msg}"
        )

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
        # 仅当查询中确实出现了目标表（别名或基表）时才进行替换，避免生成“悬空列”。
        target_available = bool(target_aliases) or (tgt_base in alias_to_base.values())
        # 选择目标前缀：优先用唯一的目标别名；否则用目标基表名；若都无则去前缀
        if len(target_aliases) == 1:
            tgt_prefix = next(iter(target_aliases))
        elif tgt_base in alias_to_base.values():
            tgt_prefix = self.target_table
        else:
            tgt_prefix = None

        if target_available:
            for col in list(tree.find_all(exp.Column)):
                t = (col.table or '').split('.')[-1].lower() if col.table else ''
                if t and t in source_aliases and col.name == self.source_column:
                    col.set("this", exp.to_identifier(self.new_column))
                    if tgt_prefix:
                        col.set("table", exp.to_identifier(tgt_prefix))
                    else:
                        col.set("table", None)

        # 步骤2：先移除 WHERE 中基于 join_keys 的等值连接条件（source_alias.skey = X.tkey 或反向），
        # 再尝试显式 JOIN 的安全删除（仅当 ON 仅包含 join_keys 等值）。之后再判断是否还存在源表引用。
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

        # 步骤3 延后：仅在确认不再引用源表其它列后，才尝试显式 JOIN 的安全删除。

        # 步骤4：重新检查是否还有源表列被引用（此时 WHERE/ON 已处理允许的等值条件）
        remaining_src_cols: set[str] = set()
        for col in list(tree.find_all(exp.Column)):
            t = (col.table or '').split('.')[-1].lower() if col.table else ''
            if t and t in source_aliases:
                remaining_src_cols.add(col.name)

        # 若仍有其他源表列被引用，则不能移除源表；仅返回列/条件清理后的 SQL
        if remaining_src_cols:
            return tree.sql(dialect='mysql')

        # —— 安全删除显式 JOIN（在确认 remaining_src_cols 为空后）——
        try:
            pairs = set(tuple(p) for p in (self.join_keys or []))
            for sel in list(tree.find_all(exp.Select)):
                joins = list(sel.args.get('joins') or [])
                if not joins:
                    continue
                new_joins = []
                for j in joins:
                    drop = False
                    if isinstance(j, exp.Join):
                        # 仅处理 INNER JOIN / 未指定 kind 的 JOIN
                        kind = j.args.get('kind')
                        kind_s = (kind.sql(dialect='mysql').strip().lower() if kind is not None else 'inner')
                        if kind is None or 'inner' in kind_s:
                            right = j.this  # joined table expr
                            right_alias = None
                            right_base = None
                            if isinstance(right, exp.Table):
                                full = right.name or ''
                                right_base = full.split('.')[-1].lower() if full else ''
                                if right.args.get('alias') is not None:
                                    right_alias = right.args['alias'].sql(dialect='mysql').split()[-1].lower()
                                else:
                                    right_alias = right_base
                            # 判断是否指向源表
                            if right_alias and right_alias in source_aliases:
                                # 校验 ON 子句：必须全部是基于 join_keys 的等值条件
                                on = j.args.get('on')
                                if on is not None:
                                    items = collect_conj(on)
                                    ok = True
                                    for n in items:
                                        if not isinstance(n, exp.EQ):
                                            ok = False; break
                                        l, r = n.left, n.right
                                        if not (isinstance(l, exp.Column) and isinstance(r, exp.Column)):
                                            ok = False; break
                                        lt = (l.table or '').split('.')[-1].lower() if l.table else ''
                                        rt = (r.table or '').split('.')[-1].lower() if r.table else ''
                                        pair_lr = (l.name, r.name)
                                        pair_rl = (r.name, l.name)
                                        if not ((lt in source_aliases and pair_lr in pairs) or (rt in source_aliases and pair_rl in pairs)):
                                            ok = False; break
                                    if ok:
                                        drop = True
                    if not drop:
                        new_joins.append(j)
                if len(new_joins) != len(joins):
                    sel.set('joins', new_joins if new_joins else None)
        except Exception:
            pass

        # 步骤5：从 FROM 中移除源表（仅一次）；保留其它项与别名
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

    def apply_to_write_sql(self, sql: str) -> str:
        """
        写入 SQL 改写：
        - INSERT INTO target_table(..., new_column, ...) VALUES (..., expr, ...)
          不变（假设 schema 已有列 new_column）。
        - INSERT INTO source_table(..., source_column, ...) 目标没有 new_column 的场景不处理（避免二义性）。
        - UPDATE target_table SET new_column = expr ...：保留。
        - UPDATE 任意表 SET source_table.source_column = ...：不改写。
        说明：RCA 在写路径无需强制改写；读路径替换列即可。留空实现为保守返回原 SQL。
        """
        return sql

    # ---------- performance eval ----------
    def evaluate_on_plan(self, plan_text: str,
                         meta_path: str | None = 'output_dir/meta.json') -> dict:
        """
        评估增加冗余列对计划代价的影响（行存储）：
        - 只影响行宽相关算子：扫描/排序/哈希聚合/物化等 → 以宽度因子缩放；
        - 本函数不改变基数（rows_factor=1）。
        - 若目标表上已经存在该列宽元数据，则直接累加；否则用源列宽近似。
        """
        try:
            from rewrite.cost_model import (
                load_meta, avg_row_width, CostModel
            )
            from performance_eval.rewrite_utils import column_avg_length
            from performance_eval.plan import parse_plan
        except Exception:
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': 'performance_eval 不可用，跳过评估'
            }

        tgt = self.target_table
        src = self.source_table
        src_col = self.source_column

        meta = load_meta(meta_path)
        w_src = column_avg_length(meta, src, src_col) or 0.0
        w_tgt = avg_row_width(meta, tgt) or 1.0
        cf = (w_tgt + w_src) / max(w_tgt, 1e-9) if w_src > 0 else 1.0

        # 仅当计划引用目标表时缩放
        nodes = parse_plan(plan_text)
        if not any(tgt in (n.tables or []) for n in nodes):
            return {
                'new_plan_text': plan_text,
                'original_total_cost': None,
                'new_total_cost': None,
                'delta': 0.0,
                'note': '计划未引用目标表，跳过'
            }
        cm = CostModel()
        res = cm.apply_type1(plan_text, target_table=tgt, rows_factor=1.0, cols_factor=cf, filter_factor=1.0)
        res['note'] = f'冗余列增加行宽，cols_factor={cf:.3g}'
        return res
