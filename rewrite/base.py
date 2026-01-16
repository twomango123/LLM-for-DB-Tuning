from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class MySQLConstraintHelper:
    """
    MySQL 约束管理辅助类，为各类 SMO 在 apply_to_schema 中重建/迁移约束提供统一能力。

    能力范围（基于 information_schema）：
    - 主键 PRIMARY KEY（提取/重建）
    - 唯一约束 UNIQUE（提取/重建）
    - 外键 FOREIGN KEY（出站/入站，提取/删除/重建）
    - CHECK 约束（MySQL 8.0+，提取/重建）
    - 列默认值/自增信息（读取列元数据，供 CTAS 后补充）
    """

    def __init__(self, db):
        self.db = db

    # ---------- 基础查询 ----------
    def _q(self, sql: str) -> List[Dict[str, Any]]:
        return self.db.execute_query(sql) if self.db and hasattr(self.db, 'execute_query') else []

    # ---------- 约束获取 ----------
    def fetch_constraints(self, table: str) -> Dict[str, Any]:
        # 主键
        pk_cols: List[str] = []
        pk_name: Optional[str] = None
        rows = self._q(f"""
            SELECT tc.CONSTRAINT_NAME, k.COLUMN_NAME
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.KEY_COLUMN_USAGE k
              ON k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             AND k.TABLE_NAME = tc.TABLE_NAME
             AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
              AND tc.TABLE_NAME = '{table}'
              AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY k.ORDINAL_POSITION
        """)
        for r in rows:
            pk_name = r['CONSTRAINT_NAME']
            pk_cols.append(r['COLUMN_NAME'])

        # 唯一约束
        uniques: List[Dict[str, Any]] = []
        u_rows = self._q(f"""
            SELECT DISTINCT tc.CONSTRAINT_NAME
            FROM information_schema.TABLE_CONSTRAINTS tc
            WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
              AND tc.TABLE_NAME = '{table}'
              AND tc.CONSTRAINT_TYPE = 'UNIQUE'
        """)
        for u in u_rows:
            name = u['CONSTRAINT_NAME']
            cols = self._q(f"""
                SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE CONSTRAINT_SCHEMA = DATABASE()
                  AND TABLE_NAME = '{table}'
                  AND CONSTRAINT_NAME = '{name}'
                ORDER BY ORDINAL_POSITION
            """)
            uniques.append({'name': name, 'columns': [c['COLUMN_NAME'] for c in cols]})

        # CHECK 约束（MySQL 8.0+）
        checks: List[Dict[str, Any]] = []
        ck_rows = self._q(f"""
            SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
            FROM information_schema.TABLE_CONSTRAINTS tc
            JOIN information_schema.CHECK_CONSTRAINTS cc
              ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
             AND cc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_SCHEMA = DATABASE()
              AND tc.TABLE_NAME = '{table}'
              AND tc.CONSTRAINT_TYPE = 'CHECK'
        """)
        for r in ck_rows:
            checks.append({'name': r['CONSTRAINT_NAME'], 'clause': r['CHECK_CLAUSE']})

        # 外键（出站）
        outbound: List[Dict[str, Any]] = []
        fk_names = self._q(f"""
            SELECT DISTINCT k.CONSTRAINT_NAME
            FROM information_schema.KEY_COLUMN_USAGE k
            JOIN information_schema.TABLE_CONSTRAINTS tc
              ON tc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
             AND tc.TABLE_NAME = k.TABLE_NAME
             AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
            WHERE k.CONSTRAINT_SCHEMA = DATABASE()
              AND k.TABLE_NAME = '{table}'
              AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
        """)
        for fk in fk_names:
            cname = fk['CONSTRAINT_NAME']
            cols = self._q(f"""
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE CONSTRAINT_SCHEMA = DATABASE()
                  AND TABLE_NAME = '{table}'
                  AND CONSTRAINT_NAME = '{cname}'
                ORDER BY ORDINAL_POSITION
            """)
            rc = self._q(f"""
                SELECT UPDATE_RULE, DELETE_RULE
                FROM information_schema.REFERENTIAL_CONSTRAINTS
                WHERE CONSTRAINT_SCHEMA = DATABASE()
                  AND CONSTRAINT_NAME = '{cname}'
                  AND TABLE_NAME = '{table}'
            """)
            outbound.append({
                'constraint_name': cname,
                'cols': [(c['COLUMN_NAME'], c['REFERENCED_TABLE_NAME'], c['REFERENCED_COLUMN_NAME']) for c in cols],
                'update_rule': rc[0]['UPDATE_RULE'] if rc else None,
                'delete_rule': rc[0]['DELETE_RULE'] if rc else None,
            })

        # 外键（入站）
        inbound: List[Dict[str, Any]] = []
        in_rows = self._q(f"""
            SELECT DISTINCT k.CONSTRAINT_NAME, k.TABLE_NAME AS CHILD_TABLE
            FROM information_schema.KEY_COLUMN_USAGE k
            WHERE k.CONSTRAINT_SCHEMA = DATABASE()
              AND k.REFERENCED_TABLE_NAME = '{table}'
        """)
        for r in in_rows:
            cname = r['CONSTRAINT_NAME']
            child = r['CHILD_TABLE']
            cols = self._q(f"""
                SELECT COLUMN_NAME, REFERENCED_COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE CONSTRAINT_SCHEMA = DATABASE()
                  AND TABLE_NAME = '{child}'
                  AND CONSTRAINT_NAME = '{cname}'
                ORDER BY ORDINAL_POSITION
            """)
            rc = self._q(f"""
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

        # 列元数据（默认值/自增等）
        col_meta = self._q(f"""
            SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, EXTRA
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = '{table}'
        """)

        return {
            'primary_key': {'name': pk_name, 'columns': pk_cols} if pk_cols else None,
            'uniques': uniques,
            'checks': checks,
            'foreign_keys_outbound': outbound,
            'foreign_keys_inbound': inbound,
            'columns': col_meta,
        }

    # ---------- 约束 SQL 构造 ----------
    @staticmethod
    def _cols_sql(cols: List[str]) -> str:
        return ", ".join(f"`{c}`" for c in cols)

    def build_add_constraints_for_table(
        self,
        new_table: str,
        original_constraints: Dict[str, Any],
        include_columns: List[str],
        rename_map: Optional[Dict[str, str]] = None,
        skip_primary_key: bool = False,
        name_prefix: Optional[str] = None,
    ) -> List[str]:
        rm = rename_map or {}
        stmts: List[str] = []

        pk = original_constraints.get('primary_key')
        if (not skip_primary_key) and pk and all((c in include_columns) for c in pk['columns']):
            cols = [rm.get(c, c) for c in pk['columns']]
            stmts.append(f"ALTER TABLE `{new_table}` ADD PRIMARY KEY ({self._cols_sql(cols)})")

        for u in original_constraints.get('uniques', []) or []:
            if all((c in include_columns) for c in u['columns']):
                cols = [rm.get(c, c) for c in u['columns']]
                cname = u['name']
                if name_prefix:
                    cname = f"{name_prefix}_{cname}"
                stmts.append(
                    f"ALTER TABLE `{new_table}` ADD CONSTRAINT `{cname}` UNIQUE ({self._cols_sql(cols)})"
                )

        for ck in original_constraints.get('checks', []) or []:
            if rm:  # 表达式重写较复杂，涉及列重命名时先跳过
                continue
            cname = ck['name']
            if name_prefix:
                cname = f"{name_prefix}_{cname}"
            stmts.append(
                f"ALTER TABLE `{new_table}` ADD CONSTRAINT `{cname}` CHECK ({ck['clause']})"
            )

        for fk in original_constraints.get('foreign_keys_outbound', []) or []:
            child_cols = [c for (c, _, _) in fk['cols']]
            if all((c in include_columns) for c in child_cols):
                cols = [rm.get(c, c) for c in child_cols]
                ref_table = fk['cols'][0][1]
                ref_cols = [rc for (_, _, rc) in fk['cols']]
                cname = fk['constraint_name']
                if name_prefix:
                    cname = f"{name_prefix}_{cname}"
                clause = (
                    f"ALTER TABLE `{new_table}` ADD CONSTRAINT `{cname}` "
                    f"FOREIGN KEY ({self._cols_sql(cols)}) REFERENCES `{ref_table}` ({self._cols_sql(ref_cols)})"
                )
                if fk['delete_rule']:
                    clause += f" ON DELETE {fk['delete_rule']}"
                if fk['update_rule']:
                    clause += f" ON UPDATE {fk['update_rule']}"
                stmts.append(clause)

        return stmts

    def build_update_inbound_fks(
        self,
        original_table: str,
        new_referenced_table: str,
        referenced_columns: List[str],
    ) -> List[str]:
        stmts: List[str] = []
        c = self.fetch_constraints(original_table)
        for fk in c.get('foreign_keys_inbound', []) or []:
            child = fk['child_table']
            cols = [ref for (_, ref) in fk['cols']]
            if all((rc in referenced_columns) for rc in cols):
                stmts.append(f"ALTER TABLE `{child}` DROP FOREIGN KEY `{fk['constraint_name']}`")
                child_cols = [c for (c, _) in fk['cols']]
                clause = (
                    f"ALTER TABLE `{child}` ADD CONSTRAINT `{fk['constraint_name']}` "
                    f"FOREIGN KEY ({self._cols_sql(child_cols)}) "
                    f"REFERENCES `{new_referenced_table}` ({self._cols_sql(cols)})"
                )
                if fk['delete_rule']:
                    clause += f" ON DELETE {fk['delete_rule']}"
                if fk['update_rule']:
                    clause += f" ON UPDATE {fk['update_rule']}"
                stmts.append(clause)
        return stmts


class SMO(ABC):
    """
    所有 Schema Modification Operator 的抽象基类
    """
    @abstractmethod
    def apply_to_schema(self, schema):
        """用于更新数据库 schema"""
        pass

    @abstractmethod
    def apply_to_sql(self, sql_ast):
        """用于根据 SMO 改写 SQL"""
        pass
