#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from DataBase.MySQLDriver import MySQLDriver  # type: ignore
except Exception:
    MySQLDriver = None  # type: ignore

try:
    from scripts.generate_meta_mysql import parse_tables_from_schema, build_meta_for_tables  # type: ignore
except Exception:
    parse_tables_from_schema = None  # type: ignore
    build_meta_for_tables = None  # type: ignore

try:
    from prompt.PART2 import build_part2  # type: ignore
except Exception:
    build_part2 = None  # type: ignore

try:
    from performance_eval.extract_samples import extract as extract_samples  # type: ignore
except Exception:
    extract_samples = None  # type: ignore


_CREATE_TABLE_RE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:(?:`?[\w]+`?\.)?`?([\w_]+)`?)\s*\(",
    re.IGNORECASE,
)


def _extract_columns(block: str) -> Dict[str, str]:
    cols: Dict[str, str] = {}
    for raw_line in block.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        up = line.upper()
        if up.startswith(("PRIMARY KEY", "FOREIGN KEY", "UNIQUE", "KEY", "CONSTRAINT")):
            continue
        m = re.match(r"`?([A-Za-z0-9_]+)`?\s+([A-Za-z]+)(?:\s*\([^)]*\))?", line)
        if not m:
            continue
        col, typ = m.group(1), m.group(2).upper()
        cols[col] = typ
    return cols


def parse_schema(schema_sql_path: Path) -> Dict[str, Dict[str, str]]:
    sql = schema_sql_path.read_text(encoding="utf-8", errors="ignore")
    tables: Dict[str, Dict[str, str]] = {}
    pos = 0
    while True:
        m = _CREATE_TABLE_RE.search(sql, pos)
        if not m:
            break
        tname = m.group(1)
        start = m.end()
        end = sql.find(");", start)
        if end == -1:
            end = sql.find(")\n", start)
            if end == -1:
                break
        block = sql[start:end]
        tables[tname] = _extract_columns(block)
        pos = end + 2
    return tables


@dataclass
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


def _connect_db(cfg: DBConfig):
    if MySQLDriver is None:
        raise RuntimeError("MySQLDriver not available")
    drv = MySQLDriver({
        "host": cfg.host,
        "port": cfg.port,
        "user": cfg.user,
        "password": cfg.password,
        "database": cfg.database,
    })
    if not drv.connect():
        raise RuntimeError("Failed to connect to MySQL")
    return drv


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_join_allowlist(workload_ops: Dict[str, Any]) -> List[Dict[str, Any]]:
    pairs: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for t, info in workload_ops.items():
        joins = info.get("join") if isinstance(info, dict) else None
        if not joins:
            continue
        for j in joins:
            other = j.get("table")
            if not other:
                continue
            a, b = (t, other) if t <= other else (other, t)
            key = (a, b)
            entry = pairs.setdefault(key, {"table_a": a, "table_b": b, "count": 0, "pairs": set()})
            try:
                entry["count"] = max(int(entry["count"]), int(j.get("count", 0) or 0))
            except Exception:
                pass
            for p in (j.get("pairs") or []):
                if not isinstance(p, (list, tuple)) or len(p) != 2:
                    continue
                if a == t:
                    entry["pairs"].add((str(p[0]), str(p[1])))
                else:
                    entry["pairs"].add((str(p[1]), str(p[0])))
    out: List[Dict[str, Any]] = []
    for _key, e in sorted(pairs.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        out.append({
            "table_a": e["table_a"],
            "table_b": e["table_b"],
            "count": int(e.get("count", 0) or 0),
            "pairs": [[a, b] for (a, b) in sorted(list(e["pairs"]))],
        })
    return out


def _collect_explain_samples(explain_dir: Path, out_dir: Path) -> Optional[Dict[str, Any]]:
    if extract_samples is None:
        return None
    if not explain_dir.exists():
        return None
    texts: List[str] = []
    for p in sorted(explain_dir.glob("*.txt")):
        try:
            texts.append(p.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            continue
    if not texts:
        return None
    raw = "\n\n".join(t for t in texts if t.strip())
    if not raw.strip():
        return None
    samples = extract_samples(raw)
    out_raw = out_dir / "samples" / "raw_explain_samples.txt"
    out_json = out_dir / "samples" / "op_cost_samples.json"
    out_raw.parent.mkdir(parents=True, exist_ok=True)
    out_raw.write_text(raw, encoding="utf-8")
    out_json.write_text(json.dumps(samples, ensure_ascii=False, indent=2), encoding="utf-8")
    return samples


def prepare_stats(
    dataset: str,
    schema_sql: Path,
    sql_dir: Path,
    out_dir: Path,
    db_cfg: Optional[DBConfig],
    sample_ratio: float,
    min_sample_rows: int,
    exec_counts_path: Optional[str],
    dml_time_cache: Optional[str],
    skip_meta: bool,
    skip_explain: bool,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    schema = parse_schema(schema_sql)
    _write_json(out_dir / "schema.json", schema)
    _write_json(out_dir / "table_columns.json", {t: sorted(list(cols.keys())) for t, cols in schema.items()})

    meta = None
    if not skip_meta:
        if db_cfg is None:
            raise SystemExit("--skip-meta 未指定且未提供数据库连接参数")
        if parse_tables_from_schema is None or build_meta_for_tables is None:
            raise SystemExit("缺少 generate_meta_mysql 依赖，无法生成 meta.json")
        tables = parse_tables_from_schema(schema_sql)
        drv = _connect_db(db_cfg)
        try:
            meta = build_meta_for_tables(drv, tables=tables, sample_ratio=sample_ratio, min_sample_rows=min_sample_rows)
        finally:
            try:
                drv.disconnect()
            except Exception:
                pass
        _write_json(out_dir / "meta.json", meta)
        if meta and isinstance(meta, dict) and meta.get("foreign_keys") is not None:
            _write_json(out_dir / "foreign_keys.json", meta.get("foreign_keys"))

    workload_ops: Optional[Dict[str, Any]] = None
    if build_part2 is None:
        raise SystemExit("缺少 prompt.PART2.build_part2，无法生成历史工作负载统计")

    debug_dir = out_dir / "part2_debug" if not skip_explain else None
    mapping_json = build_part2(
        schema_sql_path=str(schema_sql),
        sql_dir=str(sql_dir),
        dialect="mysql",
        host=db_cfg.host if db_cfg else "127.0.0.1",
        port=db_cfg.port if db_cfg else 3306,
        user=db_cfg.user if db_cfg else "root",
        password=db_cfg.password if db_cfg else "",
        database=db_cfg.database if db_cfg else "",
        config_path=None,
        debug=bool(debug_dir),
        debug_dir=str(debug_dir) if debug_dir else None,
        exec_counts_path=exec_counts_path,
        dml_time_cache=dml_time_cache,
    )
    workload_ops = json.loads(mapping_json)
    _write_json(out_dir / "workload_ops.json", workload_ops)

    join_allowlist = _build_join_allowlist(workload_ops)
    _write_json(out_dir / "join_keys.json", join_allowlist)

    explain_dir = None
    if debug_dir is not None:
        explain_dir = debug_dir / "explain"
        if explain_dir.exists():
            manifest_index = debug_dir / "index_map.json"
            if manifest_index.exists():
                _write_json(out_dir / "explain_index.json", _load_json(manifest_index))

    samples = None
    if explain_dir is not None and not skip_explain:
        samples = _collect_explain_samples(explain_dir, out_dir)

    manifest = {
        "dataset": dataset,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "inputs": {
            "schema_sql": str(schema_sql),
            "sql_dir": str(sql_dir),
        },
        "db": {
            "host": db_cfg.host if db_cfg else None,
            "port": db_cfg.port if db_cfg else None,
            "user": db_cfg.user if db_cfg else None,
            "database": db_cfg.database if db_cfg else None,
            "password_provided": bool(db_cfg and db_cfg.password),
        },
        "artifacts": {
            "schema": "schema.json",
            "table_columns": "table_columns.json",
            "meta": "meta.json" if meta is not None else None,
            "foreign_keys": "foreign_keys.json" if (meta and meta.get("foreign_keys") is not None) else None,
            "workload_ops": "workload_ops.json",
            "join_keys": "join_keys.json",
            "explain_index": "explain_index.json" if (out_dir / "explain_index.json").exists() else None,
            "samples": "samples/op_cost_samples.json" if samples is not None else None,
        },
    }
    _write_json(out_dir / "manifest.json", manifest)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Prepare normalized stats bundle for dataset")
    p.add_argument("--dataset", required=True, help="dataset name (spider/chbenchmark/etc)")
    p.add_argument("--schema-sql", required=True, help="schema.sql path")
    p.add_argument("--sql-dir", required=True, help="directory with history SQL files")
    p.add_argument("--out-dir", required=True, help="output directory for stats bundle")

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

    return p


def main(argv=None) -> None:
    ap = build_parser()
    args = ap.parse_args(argv)

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
        schema_sql=Path(args.schema_sql),
        sql_dir=Path(args.sql_dir),
        out_dir=Path(args.out_dir),
        db_cfg=db_cfg,
        sample_ratio=float(args.sample_ratio),
        min_sample_rows=int(args.min_sample_rows),
        exec_counts_path=args.exec_counts,
        dml_time_cache=args.dml_time_cache,
        skip_meta=bool(args.skip_meta),
        skip_explain=bool(args.skip_explain),
    )


if __name__ == "__main__":
    main()
