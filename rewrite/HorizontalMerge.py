try:
    from .base import SMO, MySQLConstraintHelper
except Exception:  # pragma: no cover
    from base import SMO, MySQLConstraintHelper

import re


class HorizontalMerge(SMO):
    """
    表水平合并：将多张同结构分表并回一张表。
    - sources: List[str] 源表列表
    - new_table: 目标表

    apply_to_schema: CREATE TABLE new AS SELECT * FROM s1 UNION ALL SELECT * FROM s2 ...
    apply_to_sql: 将 FROM 任一 s_i 替换为 new_table；对于包含多个 s_i 的 FROM，保留一个并移除其他重复源。
    """

    def __init__(self, sources: list[str], new_table: str):
        self.sources = sources
        self.new_table = new_table

    def apply_to_schema(self, db=None):
        """
        水平合并并按规则重建约束（MySQL 专用）：
        - UNION ALL 合并数据；
        - 主键：重建；
        - 外键：重建（出站外键，根据首个源表）；
        - 检查约束：重建；
        - 唯一约束：不重建（合并后不再保证全局唯一）；
        - 默认约束：只有当所有源表在该列上的默认相同，才在新表上复制默认；
        - 自增约束：仅当“某一列只有一个源表存在自增”时在新表上设置该列为 AUTO_INCREMENT。
        """
        union = " UNION ALL ".join([f"SELECT * FROM `{s}`" for s in self.sources])
        create = f"CREATE TABLE `{self.new_table}` AS {union};"
        stmts = ['SET FOREIGN_KEY_CHECKS=0', create]

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
