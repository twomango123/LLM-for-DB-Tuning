from __future__ import annotations

import json
from typing import Any, Dict, List
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from storage.estimator import ColumnStorageEstimator


class StubDriver:
    """Driver stub for ColumnStorageEstimator.

    It implements execute_query() to respond to:
    - listing tables
    - listing columns
    - EXPLAIN FORMAT=JSON for row count
    - sampling SQL with OCTET_LENGTH and RAND()
    """

    def __init__(self):
        self.connected = True

    def connect(self) -> bool:  # pragma: no cover - not needed
        self.connected = True
        return True

    def disconnect(self) -> bool:  # pragma: no cover - not needed
        self.connected = False
        return True

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        q = " ".join(query.strip().split()).lower()
        # list tables
        if q.startswith("select table_schema as table_schema, table_name as table_name from information_schema.tables"):
            return [
                {"table_schema": "mockdb", "table_name": "orders"},
                {"table_schema": "mockdb", "table_name": "lineitem"},
            ]
        # list columns
        if "from information_schema.columns" in q and "table_name = 'orders'" in q:
            return [
                {"column_name": "o_orderkey", "data_type": "int"},
                {"column_name": "o_orderstatus", "data_type": "char"},
            ]
        if "from information_schema.columns" in q and "table_name = 'lineitem'" in q:
            return [
                {"column_name": "l_orderkey", "data_type": "int"},
                {"column_name": "l_comment", "data_type": "varchar"},
            ]
        # explain row count
        if q.startswith("explain format=json select * from `orders`"):
            return [{"EXPLAIN": json.dumps({"query_block": {"table": {"rows": 4}}})}]
        if q.startswith("explain format=json select * from `lineitem`"):
            return [{"EXPLAIN": json.dumps({"query_block": {"table": {"rows": 5}}})}]
        # sampling stats; return a small sample
        if "select sample_row_count, sample_total_bytes, avg_single_byte, sample_ratio" in q:
            return [{
                "sample_row_count": 2,
                "sample_total_bytes": 18,
                "avg_single_byte": 9.0,
                "sample_ratio": 0.5,
            }]
        raise RuntimeError(f"Unhandled query in StubDriver: {query}")


def demo(tmp_out: Path = Path("LLM-for-DB-Tuning/output_dir/mock_storage.jsonl")):
    drv = StubDriver()
    est = ColumnStorageEstimator(drv, dialect="mysql")
    tmp_out.parent.mkdir(parents=True, exist_ok=True)
    est.estimate_tables(schema=None, tables=["orders", "lineitem"], sample_ratio=0.5, min_sample_rows=1, out_path=tmp_out)
    print("Wrote:", tmp_out)
    print(tmp_out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    demo()

