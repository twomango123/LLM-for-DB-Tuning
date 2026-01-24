#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
离线测试 PART2 功能的小脚本（无需连库）。

做法：
- 动态创建一个临时 schema.sql 和若干 SQL（含 INSERT/UPDATE/SELECT）与执行频率 CSV；
- 通过“猴子补丁”屏蔽真正的数据库访问：
  * 替换 ColumnLengthEstimator 为固定返回；
  * 替换 _connect_mysql_driver 与 _fetch_table_rows；
  * 替换 qa_analyze_sql 返回可控的 EXPLAIN 节点；
  * 可选替换 _estimate_rows_and_filtered 返回近似 rows。
- 调用 PART2.build_part2 并打印 JSON 结果。

运行：
  python3 LLM-for-DB-Tuning/prompt/test_part2_offline.py

可选参数：
  --keep-tmp   运行结束后保留临时目录，方便查看生成的 schema/sql/csv 与调试输出
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from pathlib import Path

# 导入 PART2
_THIS_DIR = Path(__file__).parent
import sys
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))
from PART2 import build_part2 as real_build_part2  # type: ignore
import PART2 as P2  # 便于猴子补丁


def _write_sample_files(base: Path) -> tuple[Path, Path, Path]:
    """写入一个最小可测的 schema.sql / sql_dir / exec_counts.csv。"""
    schema = base / "schema.sql"
    sql_dir = base / "sqls"
    sql_dir.mkdir(parents=True, exist_ok=True)
    counts = base / "exec_counts.csv"

    schema.write_text(
        """
        CREATE TABLE tpcch.orderline (
            ol_o_id integer,
            ol_d_id tinyint,
            ol_w_id integer,
            ol_number tinyint,
            ol_i_id integer,
            ol_supply_w_id integer,
            ol_delivery_d date,
            ol_quantity smallint,
            ol_amount decimal(6,2),
            ol_dist_info char(24),
            PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number)
        );
        CREATE TABLE tpcch.orders (
            o_id integer,
            o_d_id tinyint,
            o_w_id integer,
            PRIMARY KEY (o_id)
        );
        CREATE TABLE tpcch.warehouse (
            w_id integer,
            w_ytd decimal(12,2),
            PRIMARY KEY (w_id)
        );
        CREATE TABLE tpcch.history (
            h_c_id smallint,
            h_amount decimal(6,2)
        );
        CREATE TABLE tpcch.nation (
            n_nationkey tinyint,
            n_name char(25),
            n_regionkey tinyint,
            n_comment char(152),
            PRIMARY KEY(n_nationkey)
        );
        CREATE TABLE tpcch.region (
            r_regionkey tinyint,
            r_name char(55),
            PRIMARY KEY(r_regionkey)
        );
        """
        .strip(),
        encoding="utf-8",
    )

    # 只读查询（触发 join/filter/order/group）
    (sql_dir / "q_select1.sql").write_text(
        """
        SELECT ol_number
        FROM tpcch.orderline JOIN tpcch.orders
          ON orderline.ol_o_id = orders.o_id
         AND orderline.ol_d_id = orders.o_d_id
         AND orderline.ol_w_id = orders.o_w_id
        WHERE orderline.ol_i_id < 1000
        ORDER BY ol_number
        """.strip(),
        encoding="utf-8",
    )
    (sql_dir / "q_select2.sql").write_text(
        """
        SELECT n_name FROM tpcch.nation JOIN tpcch.region
          ON nation.n_regionkey = region.r_regionkey
        WHERE region.r_name = 'ASIA'
        GROUP BY n_name
        ORDER BY n_name
        """.strip(),
        encoding="utf-8",
    )

    # DML：INSERT/UPDATE
    (sql_dir / "getNoOrderlineInsert.sql").write_text(
        "insert into tpcch.orderline values (1,1,1,1,1,1,NULL,5,1.00,'x')",
        encoding="utf-8",
    )
    (sql_dir / "getPmWarehouseUpdate.sql").write_text(
        "update tpcch.warehouse set w_ytd=w_ytd+1 where w_id=1",
        encoding="utf-8",
    )
    (sql_dir / "getPmHistoryInsert.sql").write_text(
        "insert into tpcch.history values (1,1.00)",
        encoding="utf-8",
    )

    # 执行频率（无表头 filename,count）
    counts.write_text(
        "\n".join(
            [
                "q_select1.sql,263",
                "q_select2.sql,526",
                "getNoOrderlineInsert.sql,130180",
                "getPmWarehouseUpdate.sql,12439",
                "getPmHistoryInsert.sql,12439",
            ]
        ),
        encoding="utf-8",
    )
    return schema, sql_dir, counts


def _patch_offline():
    """对 PART2 做猴子补丁以离线运行。"""
    # 1) 固定列长度估计器：返回简单定值（单位：字节）
    class DummyEstimator:
        def __init__(self, _):
            pass
        def estimate_table(self, table: str, sample_ratio: float = 0.01, sample_limit: int = 10000):
            # 仅覆盖我们在样例 schema 中的列
            fixed = {
                "orderline": {
                    "ol_o_id": 4, "ol_d_id": 1, "ol_w_id": 4, "ol_number": 1,
                    "ol_i_id": 4, "ol_supply_w_id": 4, "ol_delivery_d": 3, "ol_quantity": 2,
                    "ol_amount": 3, "ol_dist_info": 24,
                },
                "orders": {"o_id": 4, "o_d_id": 1, "o_w_id": 4},
                "warehouse": {"w_id": 4, "w_ytd": 3},
                "history": {"h_c_id": 2, "h_amount": 3},
                "nation": {"n_nationkey": 1, "n_name": 25, "n_regionkey": 1, "n_comment": 152},
                "region": {"r_regionkey": 1, "r_name": 55},
            }
            return {k: {"length": v} for k, v in fixed.get(table, {}).items()}

    P2.ColumnLengthEstimator = DummyEstimator  # type: ignore

    # 2) 关闭真正的 EXPLAIN：返回可控节点
    def fake_analyze_sql(_driver, sql: str):
        nodes = []
        lw = sql.lower()
        if "from tpcch.orderline" in lw and "join tpcch.orders" in lw:
            # 三个等值连接 + 一个 filter + 一个 sort
            nodes = [
                {"text": "Filter: (orderline.ol_o_id = orders.o_id)", "avg_time": 0.5},
                {"text": "Filter: (orderline.ol_d_id = orders.o_d_id)", "avg_time": 0.4},
                {"text": "Filter: (orderline.ol_w_id = orders.o_w_id)", "avg_time": 0.3},
                {"text": "Filter: (orderline.ol_i_id < 1000)", "avg_time": 0.2},
                {"text": "Sort: orderline.ol_number", "avg_time": 0.1},
            ]
        if "from tpcch.nation" in lw and "join tpcch.region" in lw:
            nodes += [
                {"text": "Filter: (nation.n_regionkey = region.r_regionkey)", "avg_time": 0.05},
                {"text": "Filter: (region.r_name = 'ASIA')", "avg_time": 0.03},
                {"text": "Sort: nation.n_name", "avg_time": 0.02},
                {"text": "Aggregate using temporary table", "avg_time": 0.04},
            ]
        return {"raw": "(offline)", "nodes": nodes, "summary": {}}

    P2.qa_analyze_sql = fake_analyze_sql  # type: ignore

    # 3) 屏蔽数据库连接
    class FakeDriver:
        is_connected = True
        def execute_query(self, _sql: str):
            # 仅供 _fetch_table_rows 调用
            return []
        def disconnect(self):
            return True
    P2._connect_mysql_driver = lambda *a, **k: FakeDriver()  # type: ignore

    # 4) 固定表行数
    def fake_fetch_rows(_driver, _db, tables):
        base = {
            "orderline": 2940012,
            "nation": 62,
            "region": 5,
            "orders": 100000,
            "warehouse": 10,
            "history": 500000,
        }
        return {t: int(base.get(t, 1000)) for t in tables}
    P2._fetch_table_rows = fake_fetch_rows  # type: ignore

    # 5) （可选）行数估计：给部分谓词返回近似 rows，其余为 1
    def fake_est_rows_and_filtered(_est, evt):
        try:
            if evt.operation == "group by" and evt.table == "orderline" and evt.column == "ol_number":
                return 2940012.0, None
            if evt.predicate and "ol_i_id" in evt.predicate and "<" in evt.predicate:
                return 979905.0, None
        except Exception:
            pass
        return 1.0, None
    P2._estimate_rows_and_filtered = fake_est_rows_and_filtered  # type: ignore

    # 6) 关闭可选基数估计器
    P2._maybe_build_estimator = lambda *a, **k: None  # type: ignore


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep-tmp", action="store_true", help="保留临时目录以便排查调试")
    args = ap.parse_args()

    tmp = tempfile.TemporaryDirectory(prefix="part2_offline_")
    work = Path(tmp.name)
    schema, sql_dir, exec_counts = _write_sample_files(work)

    _patch_offline()

    # 开启调试，输出到临时目录下，方便查看 dml_counts.json / dml_hits.json / per_key 等
    os.environ["PART2_DEBUG"] = "1"
    os.environ["PART2_DEBUG_DIR"] = str(work / "debug")

    out_json = real_build_part2(
        schema_sql_path=str(schema),
        sql_dir=str(sql_dir),
        dialect="mysql",
        host="127.0.0.1",
        port=3306,
        user="root",
        password="",
        database="tpcch",  # 任意占位，不会真正连库
        config_path=None,
        debug=True,
        debug_dir=str(work / "debug"),
        exec_counts_path=str(exec_counts),
    )

    print("==== PART2 Offline Output ====")
    print(out_json)
    print("\n[调试目录]", work)
    if not args.keep_tmp:
        # 仅打印路径，不保留文件
        tmp.cleanup()


if __name__ == "__main__":
    main()

