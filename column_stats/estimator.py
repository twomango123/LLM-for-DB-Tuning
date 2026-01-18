from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ColumnStat:
    name: str
    length: float
    method: str  # "fixed" | "sample"
    sample_count: int = 0


class ColumnLengthEstimator:
    """MySQL 列长度估算器。

    仅支持 MySQL。需要提供已连接的 MySQLDriver 实例。
    - 定长列：按类型映射返回固定字节数；CHAR(n) 返回 n（简化）。
    - 变长列：通过抽样 (OCTET_LENGTH) 估算平均字节数。
    - 无法连库：抛出 RuntimeError。
    """

    def __init__(self, mysql_driver) -> None:
        self.db = mysql_driver
        if not getattr(self.db, "is_connected", False):
            # 明确失败：无法连库直接报错
            raise RuntimeError("MySQL 连接未建立，无法进行字段长度估算")

    # --- 定长类型映射 ---
    @staticmethod
    def _fixed_length_for_type(col_type: str, char_len: Optional[int], numeric_precision: Optional[int], numeric_scale: Optional[int]) -> Optional[int]:
        t = col_type.upper()
        # 简化：常见定长
        if t in ("TINYINT",):
            return 1
        if t in ("SMALLINT", "YEAR"):
            return 2
        if t in ("MEDIUMINT",):
            return 3
        if t in ("INT", "INTEGER", "FLOAT"):
            return 4
        if t in ("BIGINT", "DOUBLE"):
            return 8
        if t in ("DATE",):
            return 3  # MySQL 物理存储近似
        if t in ("TIME",):
            return 3
        if t in ("DATETIME", "TIMESTAMP"):
            return 8
        if t.startswith("CHAR"):
            # 简化：按 n 字节（忽略字符集差异）
            return char_len or 0
        if t.startswith("DECIMAL") or t.startswith("NUMERIC"):
            # 近似：每 9 位 ≈ 4 字节，余数映射到 0/1/2/3/4 字节
            p = numeric_precision or 0
            groups = p // 9
            rem = p % 9
            rem_bytes_map = {0: 0, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3, 6: 3, 7: 4, 8: 4}
            return groups * 4 + rem_bytes_map.get(rem, 0)
        return None

    def _fetch_columns_meta(self, table: str) -> List[Dict[str, Any]]:
        # 使用 information_schema.columns 获取列类型/长度 / 数值精度
        sql = f"""
        SELECT COLUMN_NAME as column_name,
               DATA_TYPE as data_type,
               CHARACTER_MAXIMUM_LENGTH as char_len,
               NUMERIC_PRECISION as num_precision,
               NUMERIC_SCALE as num_scale
        FROM information_schema.columns
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'
        ORDER BY ORDINAL_POSITION
        """
        return self.db.execute_query(sql)

    def _sample_avg_length(self, table: str, column: str, sample_ratio: float, sample_limit: int) -> ColumnStat:
        # 变长：OCTET_LENGTH 抽样 + 聚合
        sub = (
            f"SELECT OCTET_LENGTH(`{column}`) AS single_byte "
            f"FROM `{table}` "
            f"WHERE `{column}` IS NOT NULL AND RAND() < {sample_ratio} "
            f"LIMIT {sample_limit}"
        )
        sql = f"SELECT COUNT(*) AS sample_row_count, SUM(single_byte) AS sample_total_bytes, AVG(single_byte) AS avg_single_byte FROM ({sub}) t"
        rows = self.db.execute_query(sql)
        if not rows:
            return ColumnStat(name=column, length=0.0, method="sample", sample_count=0)
        r = rows[0]
        cnt = int(r.get("sample_row_count") or 0)
        avg_val = float(r.get("avg_single_byte") or 0.0)
        return ColumnStat(name=column, length=avg_val, method="sample", sample_count=cnt)

    def estimate_table(self, table: str, sample_ratio: float = 0.01, sample_limit: int = 10000) -> Dict[str, Dict[str, Any]]:
        """返回该表每个列的长度估计结果。

        输出：{ column: { "length": float, "method": "fixed"|"sample", "sample_count": int } }
        """
        cols = self._fetch_columns_meta(table)
        if not cols:
            raise RuntimeError(f"未获取到表结构: {table}")
        result: Dict[str, Dict[str, Any]] = {}
        for meta in cols:
            name = meta["column_name"]
            typ = (meta["data_type"] or "").upper()
            clen = meta["char_len"]
            prec = meta["num_precision"]
            scale = meta["num_scale"]

            fixed = self._fixed_length_for_type(typ, clen, prec, scale)
            if fixed is not None:
                result[name] = {"length": float(fixed), "method": "fixed", "sample_count": 0}
                continue
            # 变长：抽样
            stat = self._sample_avg_length(table, name, sample_ratio, sample_limit)
            result[name] = {"length": float(stat.length), "method": stat.method, "sample_count": stat.sample_count}
        return result

