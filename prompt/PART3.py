#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import csv
import re
from typing import Dict, List, Optional, Tuple


def _parse_query_id(val: str) -> Optional[int]:
    if not val:
        return None
    m = re.search(r"(\d+)$", str(val).strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def collect_queries(dir_path: str) -> List[Tuple[int, str]]:
    base = Path(dir_path)
    items: List[Tuple[int, str]] = []
    for p in base.iterdir():
        m = re.match(r"query(\d+)\.sql$", p.name)
        if not m or not p.is_file():
            continue
        idx = int(m.group(1))
        body = p.read_text(encoding="utf-8").rstrip()
        items.append((idx, body))
    items.sort(key=lambda x: x[0])
    return items


def load_latencies(dir_path: str, csv_path: Optional[str], unit: str = "auto") -> Optional[Dict[int, float]]:
    # Default to dir/latency_results.csv
    path = Path(csv_path) if csv_path else Path(dir_path) / "latency_results.csv"
    if not path.exists():
        return None

    raw_map: Dict[int, float] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid_raw = row.get("query_id") or row.get("id") or row.get("query") or ""
            qid = _parse_query_id(qid_raw)
            if qid is None:
                continue
            val_raw = (
                row.get("elapsed_ms") or row.get("latency_ms") or row.get("ms") or
                row.get("elapsed_s") or row.get("elapsed_sec") or row.get("seconds") or row.get("sec") or row.get("s") or ""
            )
            try:
                val = float(val_raw)
            except Exception:
                continue
            raw_map[qid] = val

    if not raw_map:
        return None

    # Unit normalization
    if unit == "auto":
        unit = "ms" if any(v >= 1000.0 for v in raw_map.values()) else "s"
    factor = 1.0 / 1000.0 if unit == "ms" else 1.0
    return {k: v * factor for k, v in raw_map.items()}


def render_queries(queries: List[Tuple[int, str]], latencies: Optional[Dict[int, float]]) -> str:
    parts: List[str] = []
    lat_map = latencies or {}
    for idx, body in queries:
        sec_str = "N/A"
        if idx in lat_map:
            secs = float(lat_map[idx])
            disp = int(round(secs))
            if secs > 0 and disp == 0:
                disp = 1
            sec_str = str(disp)
        parts.append(f"-- SQL{idx} : {sec_str} seconds --\n{body}\n\n")
    return "\n".join(parts).rstrip() + "\n"


def build_part3(sql_dir: str, csv_path: Optional[str], unit: str = "auto") -> str:
    queries = collect_queries(sql_dir)
    lat_map = load_latencies(sql_dir, csv_path, unit)
    block = render_queries(queries, lat_map)
    return (
        "历史负载及其在当前模式下的执行时间为：\n\n"
        f"{block}\n\n"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="PART3: 读取 queryN.sql + 延迟CSV，生成历史负载 + 执行时间片段")
    ap.add_argument("sql_dir", help="包含 queryN.sql 与延迟CSV 的目录（默认CSV名：latency_results.csv）")
    ap.add_argument("--csv", dest="csv_path", default=None, help="延迟CSV路径（可选，默认：sql_dir/latency_results.csv）")
    ap.add_argument("--unit", choices=["auto", "ms", "s"], default="auto", help="延迟单位（默认 auto：自动判断）")
    ap.add_argument("--out", help="输出文件；省略则打印到标准输出")
    args = ap.parse_args()

    content = build_part3(args.sql_dir, args.csv_path, args.unit)
    if args.out:
        Path(args.out).write_text(content, encoding="utf-8")
    else:
        print(content)


if __name__ == "__main__":
    main()
