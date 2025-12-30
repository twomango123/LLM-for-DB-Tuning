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
        MySQL 兼容实现，并处理与该列相关的外键约束：
        1) 查找并删除所有引用该列的外键（包含：本表作为引用端和其他表引用本表）。
        2) 执行 RENAME COLUMN。
        3) 以新列名重建外键。

        - 若提供 db（实现 execute_query/execute_statement），则执行以上语句；
          否则返回完整 SQL 脚本字符串，便于离线执行。
        """
        t = self.table
        old = self.old
        new = self.new

        stmts = []

        inbound = []   # 其他表引用本表该列
        outbound = []  # 本表作为引用端引用其他表

        def q(sql):
            if db is None or not hasattr(db, 'execute_query'):
                return []
            return db.execute_query(sql)

        # 只在有 db 时做元数据发现；无 db 则生成最小可执行脚本
        if db is not None and hasattr(db, 'execute_query'):
            # 找 inbound 外键（child -> this table.col）
            rows = q(f"""
                SELECT DISTINCT k.CONSTRAINT_NAME, k.TABLE_NAME AS CHILD_TABLE
                FROM information_schema.KEY_COLUMN_USAGE k
                WHERE k.CONSTRAINT_SCHEMA = DATABASE()
                  AND k.REFERENCED_TABLE_NAME = '{t}'
                  AND k.REFERENCED_COLUMN_NAME = '{old}'
            """)
            for r in rows:
                cname = r['CONSTRAINT_NAME']
                child = r['CHILD_TABLE']
                cols = q(f"""
                    SELECT COLUMN_NAME, REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE CONSTRAINT_SCHEMA = DATABASE()
                      AND TABLE_NAME = '{child}'
                      AND CONSTRAINT_NAME = '{cname}'
                    ORDER BY ORDINAL_POSITION
                """)
                rc = q(f"""
                    SELECT UPDATE_RULE, DELETE_RULE
                    FROM information_schema.REFERENTIAL_CONSTRAINTS
                    WHERE CONSTRAINT_SCHEMA = DATABASE()
                      AND CONSTRAINT_NAME = '{cname}'
                      AND TABLE_NAME = '{child}'
                """)
                inbound.append({
                    'child_table': child,
                    'constraint_name': cname,
                    'cols': [(c['COLUMN_NAME'], c['REFERENCED_COLUMN_NAME']) for c in cols],
                    'update_rule': rc[0]['UPDATE_RULE'] if rc else None,
                    'delete_rule': rc[0]['DELETE_RULE'] if rc else None,
                })

            # 找 outbound 外键（this table.col -> parent）
            rows = q(f"""
                SELECT DISTINCT k.CONSTRAINT_NAME
                FROM information_schema.KEY_COLUMN_USAGE k
                JOIN information_schema.TABLE_CONSTRAINTS tc
                  ON tc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
                 AND tc.TABLE_NAME = k.TABLE_NAME
                 AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
                WHERE k.CONSTRAINT_SCHEMA = DATABASE()
                  AND k.TABLE_NAME = '{t}'
                  AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
                  AND k.COLUMN_NAME = '{old}'
            """)
            for r in rows:
                cname = r['CONSTRAINT_NAME']
                cols = q(f"""
                    SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE CONSTRAINT_SCHEMA = DATABASE()
                      AND TABLE_NAME = '{t}'
                      AND CONSTRAINT_NAME = '{cname}'
                    ORDER BY ORDINAL_POSITION
                """)
                rc = q(f"""
                    SELECT UPDATE_RULE, DELETE_RULE
                    FROM information_schema.REFERENTIAL_CONSTRAINTS
                    WHERE CONSTRAINT_SCHEMA = DATABASE()
                      AND CONSTRAINT_NAME = '{cname}'
                      AND TABLE_NAME = '{t}'
                """)
                outbound.append({
                    'constraint_name': cname,
                    'cols': [(c['COLUMN_NAME'], c['REFERENCED_TABLE_NAME'], c['REFERENCED_COLUMN_NAME']) for c in cols],
                    'update_rule': rc[0]['UPDATE_RULE'] if rc else None,
                    'delete_rule': rc[0]['DELETE_RULE'] if rc else None,
                })

        # 生成执行序列
        stmts.append('SET FOREIGN_KEY_CHECKS=0')

        for fk in outbound:
            stmts.append(f"ALTER TABLE `{t}` DROP FOREIGN KEY `{fk['constraint_name']}`")
        for fk in inbound:
            stmts.append(f"ALTER TABLE `{fk['child_table']}` DROP FOREIGN KEY `{fk['constraint_name']}`")

        # 真正的重命名
        stmts.append(f"ALTER TABLE `{t}` RENAME COLUMN `{old}` TO `{new}`")

        # 重建 outbound
        for fk in outbound:
            child_cols = []
            ref_table = None
            ref_cols = []
            for col_name, rtab, rcol in fk['cols']:
                child_cols.append(f"`{new if col_name == old else col_name}`")
                ref_table = rtab
                ref_cols.append(f"`{rcol}`")
            clause = (
                f"ALTER TABLE `{t}` ADD CONSTRAINT `{fk['constraint_name']}` "
                f"FOREIGN KEY (" + ", ".join(child_cols) + ") "
                f"REFERENCES `{ref_table}` (" + ", ".join(ref_cols) + ")"
            )
            if fk['delete_rule']:
                clause += f" ON DELETE {fk['delete_rule']}"
            if fk['update_rule']:
                clause += f" ON UPDATE {fk['update_rule']}"
            stmts.append(clause)

        # 重建 inbound
        for fk in inbound:
            child_cols = [f"`{c}`" for c, _ in fk['cols']]
            ref_cols = [f"`{(new if rc == old else rc)}`" for _, rc in fk['cols']]
            clause = (
                f"ALTER TABLE `{fk['child_table']}` ADD CONSTRAINT `{fk['constraint_name']}` "
                f"FOREIGN KEY (" + ", ".join(child_cols) + ") "
                f"REFERENCES `{t}` (" + ", ".join(ref_cols) + ")"
            )
            if fk['delete_rule']:
                clause += f" ON DELETE {fk['delete_rule']}"
            if fk['update_rule']:
                clause += f" ON UPDATE {fk['update_rule']}"
            stmts.append(clause)

        stmts.append('SET FOREIGN_KEY_CHECKS=1')

        if db is not None and hasattr(db, 'execute_statement'):
            ok = True
            for s in stmts:
                ok = ok and db.execute_statement(s)
            return ok
        return "\n".join(stmts)

    def apply_to_sql(self, sql: str) -> str:
        """把查询中引用到的旧列名改为新列名，保留表前缀/别名。"""
        try:
            tree = parse_one(sql)

            for col in tree.find_all(exp.Column):
                # 仅在列名精确匹配时替换，保留原来的表/别名
                if col.name == self.old:
                    col.set("this", exp.to_identifier(self.new))

            return tree.sql()
        except Exception:
            # 解析失败则使用稳妥的词边界替换作为兜底
            pattern = rf"(?<!\.)\b{re.escape(self.old)}\b"
            return re.sub(pattern, self.new, sql)

    