#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
from pathlib import Path
import sys

import json
from stats.prepare import prepare_stats, DBConfig


def _load_dataset_defaults(name: str) -> dict:
    cfg_path = Path(__file__).resolve().parent / 'stats' / 'datasets.json'
    if not cfg_path.exists():
        return {}
    try:
        data = json.loads(cfg_path.read_text(encoding='utf-8'))
    except Exception:
        return {}
    return data.get(name, {}) if isinstance(data, dict) else {}


def _add_prepare_stats(sub: argparse._SubParsersAction) -> None:
    p = sub.add_parser("prepare-stats", help="collect normalized stats bundle")
    p.add_argument("--dataset", required=True, help="dataset name (spider/chbenchmark/etc)")
    p.add_argument("--schema-sql", default=None, help="schema.sql path (optional if dataset config provides)")
    p.add_argument("--sql-dir", default=None, help="directory with history SQL files (optional if dataset config provides)")
    p.add_argument("--out-dir", default=None, help="output directory for stats bundle (optional if dataset config provides)")
    p.add_argument("--dataset-config", default=None, help="optional dataset config json (default: stats/datasets.json)")

    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="")
    p.add_argument("--database", default="")

    p.add_argument("--sample-ratio", type=float, default=0.01)
    p.add_argument("--min-sample-rows", type=int, default=100)

    p.add_argument("--exec-counts", default=None, help="optional exec count csv for workload weighting")
    p.add_argument("--dml-time-cache", default=None, help="optional DML timing cache json")

    p.add_argument("--skip-meta", action="store_true", help="skip meta.json generation")
    p.add_argument("--skip-explain", action="store_true", help="skip explain analyze and sample extraction")
    p.set_defaults(func=_cmd_prepare_stats)


def _cmd_prepare_stats(args: argparse.Namespace) -> None:
    defaults = {}
    if args.dataset_config:
        try:
            data = json.loads(Path(args.dataset_config).read_text(encoding='utf-8'))
            if isinstance(data, dict):
                defaults = data.get(args.dataset, {}) or {}
        except Exception:
            defaults = {}
    if not defaults:
        defaults = _load_dataset_defaults(args.dataset)

    schema_sql = args.schema_sql or defaults.get('schema_sql')
    sql_dir = args.sql_dir or defaults.get('sql_dir')
    out_dir = args.out_dir or defaults.get('out_dir')
    exec_counts = args.exec_counts or defaults.get('exec_counts')
    dml_time_cache = args.dml_time_cache or defaults.get('dml_time_cache')

    if not schema_sql or not sql_dir or not out_dir:
        raise SystemExit("缺少必要路径：schema_sql / sql_dir / out_dir（可通过 dataset config 提供）")

    db_cfg = None
    if not args.skip_meta or not args.skip_explain:
        if not args.database:
            raise SystemExit("需要 --database 以连接 MySQL")
        db_cfg = DBConfig(
            host=args.host,
            port=int(args.port),
            user=args.user,
            password=args.password,
            database=args.database,
        )

    prepare_stats(
        dataset=args.dataset,
        schema_sql=Path(schema_sql),
        sql_dir=Path(sql_dir),
        out_dir=Path(out_dir),
        db_cfg=db_cfg,
        sample_ratio=float(args.sample_ratio),
        min_sample_rows=int(args.min_sample_rows),
        exec_counts_path=exec_counts,
        dml_time_cache=dml_time_cache,
        skip_meta=bool(args.skip_meta),
        skip_explain=bool(args.skip_explain),
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="LLM-for-DB-Tuning CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    _add_prepare_stats(sub)

    return p


def main(argv: list[str] | None = None) -> None:
    ap = build_parser()
    args = ap.parse_args(argv)
    if not hasattr(args, "func"):
        ap.print_help()
        raise SystemExit(2)
    args.func(args)


if __name__ == "__main__":
    main()
