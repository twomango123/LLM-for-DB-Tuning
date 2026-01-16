from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional
import json
import math
from pathlib import Path


def pg_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def my_ident(name: str) -> str:
    return '`' + str(name).replace('`', '``') + '`'


PG_FIXED_SIZES = {
    # 常见定长类型估算（未考虑 TOAST/压缩、对齐开销）
    "smallint": 2,
    "int2": 2,
    "integer": 4,
    "int4": 4,
    "bigint": 8,
    "int8": 8,
    "real": 4,
    "float4": 4,
    "double precision": 8,
    "float8": 8,
    "boolean": 1,
    "date": 4,
    "time": 8,
    "timestamp": 8,
}

MYSQL_FIXED_SIZES = {
    # 粗略估计（MySQL 变长/变编码较多，建议以抽样为准）
    "tinyint": 1,
    "smallint": 2,
    "mediumint": 3,
    "int": 4,
    "integer": 4,
    "bigint": 8,
    "float": 4,
    "double": 8,
    "date": 3,
    "datetime": 5,  # 5-8 bytes; 取下限近似
    "timestamp": 4,
    "time": 3,
    "year": 1,
    "bool": 1,
    "boolean": 1,
}


def is_varlen_type(dialect: str, data_type: str) -> bool:
    dt = data_type.lower()
    if dialect == "postgres":
        return any(x in dt for x in ["char", "text", "json", "bytea", "varchar"])
    else:
        return any(x in dt for x in ["char", "text", "json", "blob", "varchar", "varbinary"])


def fixed_size_bytes(dialect: str, data_type: str) -> Optional[int]:
    dt = data_type.lower()
    if dialect == "postgres":
        return PG_FIXED_SIZES.get(dt)
    else:
        return MYSQL_FIXED_SIZES.get(dt)


class ColumnStorageEstimator:
    """列级存储开销估计。

    依赖驱动提供：connect()/disconnect()/execute_query()。
    
    - PostgreSQL：抽样字节使用 pg_column_size(col)
    - MySQL：抽样字节使用 OCTET_LENGTH(col)
    """

    def __init__(self, driver, dialect: str = "mysql"):
        self.db = driver
        self.dialect = dialect

    # ---------- 元数据 ----------
    def list_tables(self, schema: Optional[str] = None, tables: Optional[Iterable[str]] = None) -> List[Dict[str, str]]:
        # MySQL 当前数据库
        sql = f"""
            SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        """
        rows = self.db.execute_query(sql)
        out = [{"table_schema": r["table_schema"], "table_name": r["table_name"]} for r in rows]
        if tables:
            keep = set(tables)
            out = [t for t in out if t["table_name"] in keep]
        return out

    def list_columns(self, schema: Optional[str], table: str) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT COLUMN_NAME AS column_name, DATA_TYPE AS data_type
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
        """
        return self.db.execute_query(sql)

    # ---------- 行数估计 ----------
    def estimate_row_count(self, schema: Optional[str], table: str) -> Optional[int]:
        try:
            # MySQL: EXPLAIN FORMAT=JSON
            rows = self.db.execute_query(f"EXPLAIN FORMAT=JSON SELECT * FROM `{table}`")
            raw = rows[0].get("EXPLAIN") if isinstance(rows[0], dict) else rows[0][0]
            qb = json.loads(raw).get("query_block", {})
            tbl = qb.get("table") or {}
            rp = tbl.get("rows_produced_per_join")
            if rp is not None:
                return int(rp)
            base = tbl.get("rows")
            if base is not None:
                return int(base)
            return None
        except Exception:
            return None

    # ---------- 抽样估计（变长） ----------
    def sample_column_bytes(self, schema: Optional[str], table: str, column: str, sample_ratio: float) -> Dict[str, Any]:
        # MySQL: 使用 RAND() 抽样与 OCTET_LENGTH
        tq = my_ident(table)
        cq = my_ident(column)
        sql = f"""
            WITH sample_stats AS (
                SELECT OCTET_LENGTH({cq}) AS single_byte, {sample_ratio} AS sample_ratio
                FROM {tq}
                WHERE RAND() < {sample_ratio}
            ),
            sample_total AS (
                SELECT SUM(single_byte) AS sample_total_bytes,
                       COUNT(*) AS sample_row_count,
                       AVG(single_byte) AS avg_single_byte,
                       sample_ratio
                FROM sample_stats
                GROUP BY sample_ratio
            )
            SELECT sample_row_count, sample_total_bytes, avg_single_byte, sample_ratio
            FROM sample_total
        """
        rows = self.db.execute_query(sql)
        return rows[0] if rows else {"sample_row_count": 0, "sample_total_bytes": 0, "avg_single_byte": None, "sample_ratio": sample_ratio}

    # ---------- 主流程 ----------
    def estimate_tables(self,
                        schema: Optional[str],
                        tables: Optional[Iterable[str]],
                        sample_ratio: float,
                        min_sample_rows: int,
                        out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            for t in self.list_tables(schema, tables):
                sch = t.get("table_schema")
                name = t.get("table_name")
                est_rows = self.estimate_row_count(None, name)
                # 动态调整抽样比例以满足最小样本数
                eff_ratio = sample_ratio
                if est_rows and min_sample_rows and sample_ratio > 0:
                    try:
                        needed = float(min_sample_rows) / float(est_rows)
                        if needed > eff_ratio:
                            eff_ratio = min(1.0, needed)
                    except Exception:
                        pass
                cols = self.list_columns(None, name)
                for c in cols:
                    col = c["column_name"]
                    dt = c["data_type"]
                    is_fixed = not is_varlen_type(self.dialect, dt)
                    record: Dict[str, Any] = {
                        "schema": sch,
                        "table": name,
                        "column": col,
                        "data_type": dt,
                        "is_fixed": is_fixed,
                        "estimated_row_count": est_rows,
                    }
                    if is_fixed:
                        size = fixed_size_bytes(self.dialect, dt)
                        record["bytes_per_value"] = size
                        if size is not None and est_rows is not None:
                            record["estimated_total_bytes"] = size * int(est_rows)
                            record["method"] = "fixed_length"
                        else:
                            record["estimated_total_bytes"] = None
                            record["method"] = "fixed_length_partial"
                    else:
                        s = self.sample_column_bytes(sch if self.dialect == "postgres" else None, name, col, eff_ratio)
                        record.update({
                            "sample_ratio": s.get("sample_ratio"),
                            "sample_row_count": s.get("sample_row_count"),
                            "sample_total_bytes": s.get("sample_total_bytes"),
                            "avg_bytes_per_value": s.get("avg_single_byte"),
                        })
                        # 用总样本字节除以比例近似全列字节；若无样本，用 avg * 行数 兜底
                        total_bytes = None
                        sr = s.get("sample_ratio") or sample_ratio
                        cnt = s.get("sample_row_count")
                        sum_bytes = s.get("sample_total_bytes")
                        avg_b = s.get("avg_single_byte")
                        if cnt and sum_bytes and sr:
                            try:
                                total_bytes = int(sum_bytes / float(sr))
                            except Exception:
                                total_bytes = None
                        elif avg_b is not None and est_rows is not None:
                            total_bytes = int(float(avg_b) * int(est_rows))
                        record["estimated_total_bytes"] = total_bytes
                        record["method"] = "sample_estimate"

                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
