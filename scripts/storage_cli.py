#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Dict

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.storage_transformer import StorageModel  # type: ignore


def load_meta(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def save_meta(path: Path, meta: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def format_total(total: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    val = float(total)
    i = 0
    while val >= 1024 and i < len(units) - 1:
        val /= 1024.0
        i += 1
    return f"{val:.2f}{units[i]}"


def cmd_total(args) -> None:
    meta = load_meta(Path(args.input))
    model = StorageModel(meta)
    total = model.compute_total_storage()
    # 更新写回（可选）
    model.update_total_storage()
    if args.output:
        save_meta(Path(args.output), model.meta)
    print(f"初始总存储大小：{format_total(int(total))}")


def cmd_apply(args) -> None:
    meta = load_meta(Path(args.input))
    model = StorageModel(meta)
    init_total = int(model.meta.get("total_storage_bytes") or model.compute_total_storage())
    if args.op:
        updated = model.apply(args.op)
    else:
        updated = model.meta
    new_total = int(updated.get("total_storage_bytes") or model.compute_total_storage())
    if args.output:
        save_meta(Path(args.output), updated)
    print(f"{args.label or '操作'}后总存储大小：{format_total(new_total)}")
    print(json.dumps({
        "before": init_total,
        "after": new_total,
        "delta": new_total - init_total
    }, ensure_ascii=False))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="存储开销计算与操作后总量评估")
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("total", help="计算初始总存储大小")
    p1.add_argument("--input", required=True, help="meta.json 路径")
    p1.add_argument("--output", help="如提供则写回更新后的 meta.json（含 total_storage_bytes）")
    p1.set_defaults(func=cmd_total)

    p2 = sub.add_parser("apply", help="应用单个操作并输出操作后总存储大小")
    p2.add_argument("--input", required=True, help="meta.json 路径")
    p2.add_argument("--op", required=True, help="操作字符串，参考 response/runner.py 及 storage_transformer 约定")
    p2.add_argument("--label", help="输出文案标签（默认：操作）")
    p2.add_argument("--output", help="如提供则写回更新后的 meta.json")
    p2.set_defaults(func=cmd_apply)

    return p


def main(argv=None):
    ap = build_parser()
    args = ap.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()

