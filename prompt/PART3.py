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
        # Support both queryN.sql and query_XX.sql (with leading zeros)
        m = re.match(r"query_?(\d+)\.sql$", p.name)
        if not m or not p.is_file():
            continue
        idx = int(m.group(1))
        body = p.read_text(encoding="utf-8").rstrip()
        items.append((idx, body))
    items.sort(key=lambda x: x[0])
    return items

def _load_latencies_csv(path: Path) -> Optional[Dict[int, float]]:
    """
    解析 CSV 为毫秒（ms）。优先读取 *_ms 列；若只有 *_s 列，则换算为 ms。
    支持列名：query_id/id/query + [elapsed_ms|latency_ms|ms] 或 [elapsed_s|elapsed_sec|seconds|sec|s]
    返回字典：{query_id: 毫秒数}
    """
    out_ms: Dict[int, float] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid_raw = row.get("query_id") or row.get("id") or row.get("query") or ""
            qid = _parse_query_id(qid_raw)
            if qid is None:
                continue
            val_ms: Optional[float] = None
            # 优先毫秒列
            for k in ("elapsed_ms", "latency_ms", "ms"):
                v = row.get(k)
                if v is None:
                    continue
                try:
                    val_ms = float(v)
                    break
                except Exception:
                    pass
            # 其次秒列，换算为 ms
            if val_ms is None:
                for k in ("elapsed_s", "elapsed_sec", "seconds", "sec", "s"):
                    v = row.get(k)
                    if v is None:
                        continue
                    try:
                        val_ms = float(v) * 1000.0
                        break
                    except Exception:
                        pass
            if val_ms is None:
                continue
            out_ms[qid] = val_ms
    return out_ms or None


def _load_latencies_txt(path: Path) -> Optional[Dict[int, float]]:
    """
    Parse lines like: 'Q1, success, 832 ms' (unit in ms). Returns seconds.
    Ignores headers and separators.
    """
    txt = path.read_text(encoding="utf-8")
    mapping: Dict[int, float] = {}
    for line in txt.splitlines():
        line = line.strip()
        if not line or line.startswith("-"):
            continue
        m = re.match(r"^Q(\d+)\s*,\s*[^,]*\s*,\s*([\d.]+)\s*(ms|millisecond[s]?|s|sec|second[s]?)?\s*$", line, re.IGNORECASE)
        if not m:
            continue
        qid = int(m.group(1))
        val = float(m.group(2))
        unit = (m.group(3) or "ms").lower()
        # 输出统一为 ms
        if unit.startswith("ms"):
            ms = val
        else:
            ms = val * 1000.0
        mapping[qid] = ms
    return mapping or None

def load_latencies(latency_path: str) -> Optional[Dict[int, float]]:
    """从指定文件加载延迟，输出为毫秒（ms）。支持 .csv 与 .txt。"""
    path = Path(latency_path)
    if not path.exists():
        return None
    ext = path.suffix.lower()
    if ext == ".csv":
        return _load_latencies_csv(path)
    if ext == ".txt":
        return _load_latencies_txt(path)
    # Fallback: attempt CSV first, then TXT parsing
    return _load_latencies_csv(path) or _load_latencies_txt(path)


def render_queries(queries: List[Tuple[int, str]], latencies_ms: Optional[Dict[int, float]]) -> str:
    parts: List[str] = []
    lat_map = latencies_ms or {}
    for idx, body in queries:
        ms_str = "N/A"
        if idx in lat_map:
            ms = float(lat_map[idx])
            disp = int(round(ms))
            if ms > 0 and disp == 0:
                disp = 1
            ms_str = str(disp)
        parts.append(f"-- SQL{idx} : {ms_str} ms --\n{body}\n\n")
    return "\n".join(parts).rstrip() + "\n"


def build_part3(sql_dir: str, latency_path: Optional[str]) -> str:
    queries = collect_queries(sql_dir)
    if not latency_path:
        raise SystemExit("需要提供延迟结果文件路径（CSV或TXT）")
    lat_map = load_latencies(latency_path)
    block = render_queries(queries, lat_map)
    return (
        "历史负载及其在当前模式下的执行时间为：\n\n"
        f"~~~sql\n{block}~~~\n"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="PART3: 读取 queryN.sql (或 query_XX.sql) + 指定延迟结果(CSV/TXT)，生成历史负载 + 执行时间片段（单位：ms）")
    ap.add_argument("sql_dir", help="包含 queryN.sql/ query_XX.sql 的目录")
    ap.add_argument("--latency", "--csv", dest="latency_path", required=True, help="延迟结果文件路径（CSV 或 TXT）")
    ap.add_argument("--out", help="输出文件；省略则打印到标准输出")
    args = ap.parse_args()

    content = build_part3(args.sql_dir, args.latency_path)
    if args.out:
        Path(args.out).write_text(content, encoding="utf-8")
    else:
        print(content)


if __name__ == "__main__":
    main()
