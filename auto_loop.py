#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Auto loop orchestrator for LLM-driven schema rewrite.

Workflow:
- Step1: Read prompt/prompt.md (or fallback) and send as the first message to the LLM.
- Save the reply to response/response1.txt (and mirror to response/response.txt).
- Step2: Run response/runner.py to apply/plan the rewrite; capture outputs.
- If errors are detected, wrap error logs as feedback and ask LLM to revise.
- Save each revised reply to response/responseN.txt, mirror to response/response.txt, and loop.
- Stop when runner reports success (no error keywords) or max iterations reached.

Usage:
  python3 LLM-for-DB-Tuning/auto_loop.py \
      [--max-iters 5] [--use-db] [--host HOST] [--port 3306] [--user root] [--password ''] [--database ''] \
      [--sql-dir INPUT_SQL_DIR] [--out-sql-dir OUTPUT_SQL_DIR] \
      [--schema-sql SCHEMA_SQL] [--storage-meta META_JSON] [--storage-budget 10GB|500MB|BYTES]

Notes:
- Uses llm.llm.call_llm_api, which is configured with URL/model/api_key.
- Runner reads response/response.txt; we keep it synced with response/responseN.txt per iteration.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Tuple, Any, Set

import aiohttp

# Prefer the existing LLM caller
try:
    from llm.llm import call_llm_api  # type: ignore
except Exception as e:  # pragma: no cover
    print(f"[auto-loop] 无法导入 llm.llm.call_llm_api: {e}")
    raise


# ------------------------------
# 离线性能反馈工具（采用新估算器 + DML 写入估算）
# 说明：旧版 run_ops_on_plan 已弃用，不再用于任何反馈或中间结果。
# ------------------------------
try:
    from performance_eval.ops import parse_ops  # type: ignore
    from performance_eval.dml_models import TableStat, op_level_dml_estimate  # type: ignore
except Exception:
    parse_ops = None  # type: ignore

def _safe_load_json(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    try:
        import json
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except Exception:
        return None

def _build_perf_feedback(ops_text: str, explain_dir: str, storage_meta_path: Optional[str] = None,
                         prune_patterns: Optional[List[str]] = None, max_plans: int = 12) -> Tuple[str, Dict[str, Any]]:
    """使用新估算器（EXPLAIN cost + 存储元数据缩放）生成简要性能反馈。
    注意：旧版 run_ops_on_plan 已移除，不再展示“按操作聚合”的旧摘要。
    返回 (summary_md, details_dict)。"""
    # 1) 运行基于 explain_debug 的离线估算
    pred, det = _static_cost_eval_explain(explain_dir, sql_dir=str(Path(explain_dir)/'sql'), ops_text=ops_text,
                                          init_meta=_safe_load_json(storage_meta_path) or None)
    lines: List[str] = []
    per_query = det.get('per_query') if isinstance(det, dict) else []
    # 受影响覆盖率：pred 与 base 或 elim_frac/scan缩放有变化的计划数
    affected_plans: List[str] = []
    total_count = 0
    if isinstance(per_query, list):
        total_count = len(per_query)
        for i, q in enumerate(per_query, 1):
            try:
                b = float(q.get('baseline_s', 0.0) or 0.0)
                p = float(q.get('pred_s', 0.0) or 0.0)
                ef = float(q.get('elim_frac', 0.0) or 0.0)
                if abs(p - b) > 1e-9 or ef > 0.0:
                    affected_plans.append(f"q{q.get('qid')}.txt" if q.get('qid') is not None else f"q{i}.txt")
            except Exception:
                pass
    affected_count = len(affected_plans)
    if total_count > 0:
        pct = 100.0 * affected_count / float(total_count)
        lines.append(f"受影响计划覆盖率：{affected_count}/{total_count}（{pct:.1f}%）；样本：{', '.join(affected_plans[:5])}{' …' if len(affected_plans)>5 else ''}")

    # 成本对比（cost 单位）
    try:
        base = float(det.get('baseline_total_cost', 0.0))
        pred = float(det.get('pred_total_cost', 0.0))
        imp = float(det.get('improvement_cost', 0.0))
        lines.append('性能评估对比（cost）：')
        lines.append(f"- 初始基线：{base:.0f} cost")
        lines.append(f"- 预测结果：{pred:.0f} cost（改善 {imp:.0f}）")
    except Exception:
        pass

    # 代表性查询（Top-3 按 |delta|）
    topq: List[Tuple[str, float]] = []
    try:
        for q in per_query or []:
            b = float(q.get('baseline_s', 0.0) or 0.0)
            p = float(q.get('pred_s', 0.0) or 0.0)
            dt = p - b
            qn = f"q{q.get('qid')}.txt" if q.get('qid') is not None else 'q?.txt'
            topq.append((qn, dt))
        topq.sort(key=lambda x: abs(x[1]), reverse=True)
        if topq:
            lines.append('代表性查询净效应：')
            for qn, dt in topq[:3]:
                sign = '下降' if dt < 0 else '上升'
                lines.append(f"- {qn}: 总成本{sign} {abs(dt):.3g}")
    except Exception:
        pass

    # 2) DML 写入估算（保留）
    dml_lines: List[str] = []
    meta = _safe_load_json(storage_meta_path)
    if meta is not None and parse_ops is not None:
        table_stats: Dict[str, TableStat] = {}
        for t, info in (meta.get('tables') or {}).items():
            try:
                rows = float(info.get('row_count', 0) or 0)
                nidx = int(info.get('secondary_indexes', 1) or 1)
                table_stats[str(t)] = TableStat(rows=rows, n_secondary_indexes=nidx)
            except Exception:
                pass
        try:
            for i, op in enumerate(parse_ops(ops_text), 1):
                o = {'kind': op.kind, **(op.args or {})}
                est = op_level_dml_estimate(o, table_stats)
                ins, upd = est.get('insert_cost', 0.0), est.get('update_cost', 0.0)
                if ins or upd:
                    tags = []
                    if ins: tags.append(f"INSERT≈{ins:.3g}")
                    if upd: tags.append(f"UPDATE≈{upd:.3g}")
                    dml_lines.append(f"OP{i} {op.kind}: "+", ".join(tags))
                    if len(dml_lines) >= 4:
                        break
        except Exception:
            pass
    if dml_lines:
        lines.append('写入开销估计：')
        lines.extend(['- ' + s for s in dml_lines])

    summary = "\n".join(lines[:32])
    details: Dict[str, Any] = {'explain_cost': det, 'dml': dml_lines}

    # 3)（可选）融合 runner 的逐步评估，生成“逐操作单步/累积 delta 汇总”供 LLM 精细对照
    try:
        from pathlib import Path as _Path
        import json as _json
        jpath = _Path(RESPONSE_DIR) / 'auto_eval_runner.json'
        if jpath.exists():
            data = _json.loads(jpath.read_text(encoding='utf-8'))
            # 预解析当前 ops 的步骤名称（用于展示）
            op_names: list[str] = []
            try:
                from performance_eval.ops import parse_ops as _pops  # type: ignore
                for i, _op in enumerate(_pops(ops_text), 1):
                    op_names.append(_op.kind)
            except Exception:
                pass
            # 汇总每个 step 的 delta_step 与 cumulative_delta、命中率(sql_changed)
            agg: dict[int, dict[str, list[float]]] = {}
            hits: dict[int, int] = {}
            totals: dict[int, int] = {}
            for plan_res in (data or {}).values():
                steps = (plan_res or {}).get('steps') or []
                for idx, st in enumerate(steps):
                    totals[idx] = totals.get(idx, 0) + 1
                    if st.get('sql_changed') is True:
                        hits[idx] = hits.get(idx, 0) + 1
                    bucket = agg.setdefault(idx, {'delta_step': [], 'cumulative_delta': []})
                    if st.get('delta_step') is not None:
                        try:
                            bucket['delta_step'].append(float(st['delta_step']))
                        except Exception:
                            pass
                    if st.get('cumulative_delta') is not None:
                        try:
                            bucket['cumulative_delta'].append(float(st['cumulative_delta']))
                        except Exception:
                            pass
            def _median(a: list[float]) -> float:
                if not a:
                    return 0.0
                b = sorted(a)
                n = len(b)
                m = n // 2
                return (b[m] if n % 2 == 1 else 0.5 * (b[m-1] + b[m]))
            lines2: list[str] = []
            lines2.append('逐操作效果（基于 runner per-step 汇总）：')
            for idx in sorted(agg.keys()):
                nm = op_names[idx] if idx < len(op_names) else f'OP{idx+1}'
                ds = _median(agg[idx]['delta_step'])
                cd = _median(agg[idx]['cumulative_delta'])
                hr = 0.0
                if totals.get(idx):
                    hr = 100.0 * float(hits.get(idx, 0)) / float(totals.get(idx, 1))
                sign_ds = '下降' if ds < 0 else '上升'
                lines2.append(f"- 第{idx+1}步 {nm}: 单步{sign_ds} {abs(ds):.3g}，累计 {cd:+.3g}；命中改写比≈{hr:.1f}%")
            if len(lines2) > 1:
                summary = summary + "\n\n" + "\n".join(lines2)
                details['per_step_summary'] = lines2
    except Exception:
        pass

    return summary, details


def _infer_prune_patterns_from_ops(ops_text: str) -> List[str]:
    """从 LLM 的操作序列中自动提取可用于类型3的过滤模式。
    当前支持：HorizontalSplit(table, [(new, where_sql), ...]) 中的 where_sql 简单等值/范围条件。
    返回的模式将直接用于匹配 EXPLAIN Filter 行（子串匹配）。
    """
    pats: List[str] = []
    if not ops_text:
        return pats
    import re as _re
    for ln in [ln.strip() for ln in ops_text.splitlines() if ln.strip()]:
        if 'HorizontalSplit' not in ln:
            continue
        m = _re.search(r"HorizontalSplit\((.*)\)$", ln)
        if not m:
            continue
        body = m.group(1)
        # 抽取 ('new','WHERE') 对
        for new, where in _re.findall(r"\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)", body):
            w = where.strip()
            # 进一步抽取简单谓词 a op b（op 属于 =,>=,<=,>,<）
            for col, op, val in _re.findall(r"([A-Za-z_][A-Za-z0-9_\.]*?)\s*(=|>=|<=|>|<)\s*([^\s]+)", w):
                # 直接把片段拼回作为匹配模式
                frag = f"{col} {op} {val}"
                if frag not in pats:
                    pats.append(frag)
    return pats


ROOT = Path(__file__).resolve().parent  # LLM-for-DB-Tuning
PROMPT_DIR = ROOT / 'prompt'
# 可在运行时通过 --out-base-dir/--run-tag 重定向到其它输出目录
RESPONSE_DIR = ROOT / 'response'
REPLIES_DIR = RESPONSE_DIR / 'replies'
LOGS_DIR = RESPONSE_DIR / 'logs'
# Dedicated folder to store per-run intermediate performance feedback files
PERF_FB_DIR = RESPONSE_DIR / 'perf_fb'

def _set_output_dirs(base: Path) -> None:
    """Override global output dirs to a new base dir and ensure structure."""
    global RESPONSE_DIR, REPLIES_DIR, LOGS_DIR, PERF_FB_DIR
    RESPONSE_DIR = base
    REPLIES_DIR = RESPONSE_DIR / 'replies'
    LOGS_DIR = RESPONSE_DIR / 'logs'
    PERF_FB_DIR = RESPONSE_DIR / 'perf_fb'
    REPLIES_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    PERF_FB_DIR.mkdir(parents=True, exist_ok=True)

def _clean_output_dir(base: Path) -> None:
    """Clean previous auto-loop artifacts inside base dir for a fresh run.
    Preserves non-default folders (e.g., backups) if present.
    """
    import shutil
    base.mkdir(parents=True, exist_ok=True)
    # remove common files
    for pat in [
        'response.txt','success.txt','failure.txt','summary.csv',
        'runner_*.log','perf_fb_*.md','eval_static_*.json','eval_static_m*_r*.json',
    ]:
        for p in base.glob(pat):
            try:
                p.unlink()
            except Exception:
                pass
    # remove folders replies/logs/out_sql/perf_fb if exist
    for sub in ['replies','logs','out_sql','perf_fb']:
        d = base / sub
        if d.exists():
            try:
                shutil.rmtree(d)
            except Exception:
                pass
    # recreate minimal structure
    (base / 'replies').mkdir(parents=True, exist_ok=True)
    (base / 'logs').mkdir(parents=True, exist_ok=True)
    (base / 'perf_fb').mkdir(parents=True, exist_ok=True)
RUNNER = RESPONSE_DIR / 'runner.py'


def _read_initial_prompt() -> tuple[str, Path]:
    """Find and read the initial prompt file.
    Search order:
    - prompt/prompt.md
    - prompt/final_prompt.md
    - final_prompt.md (project root)
    - prompt.md (project root)
    """
    candidates = [
        PROMPT_DIR / 'prompt.md',
        PROMPT_DIR / 'final_prompt.md',
        ROOT / 'final_prompt.md',
        ROOT / 'prompt.md',
    ]
    for p in candidates:
        if p.exists():
            try:
                return p.read_text(encoding='utf-8'), p
            except Exception:
                pass
    raise FileNotFoundError("未找到初始提示文件：prompt/prompt.md 或 final_prompt.md")


def _sanitize_llm_reply_to_ops(text: str) -> str:
    """Try to extract only operation lines for the runner from LLM output.
    - Strip code fences and surrounding commentary.
    - Keep lines that start with known operation keywords or chain with '-'.
    - If nothing matches, return original text.
    """
    s = text.strip()
    # Remove common code fences
    s = re.sub(r"```[a-zA-Z0-9_-]*\n|```", "\n", s)
    lines = [ln.strip() for ln in s.splitlines() if ln.strip()]
    op_prefixes = (
        'VerticalSplit(', 'HorizontalSplit(', 'HorizontalMerge(',
        'RedundantColumnAdd(', 'RedundantColumnDrop(', 'TableJoin('
    )
    picked: list[str] = []
    for ln in lines:
        if ln.startswith(op_prefixes) or ' - ' in ln or ln.count('VerticalSplit(')+ln.count('TableJoin(')+ln.count('HorizontalSplit(')+ln.count('HorizontalMerge(')+ln.count('RedundantColumnAdd(')+ln.count('RedundantColumnDrop(') > 1:
            picked.append(ln.rstrip(';'))
    if not picked and lines:
        # fallback: pick the longest line; often the op chain is a single long line
        longest = max(lines, key=len)
        picked = [longest]
    return ' \n'.join(picked)


def _detect_success(stdout: str, stderr: str, returncode: int) -> tuple[bool, str]:
    out = (stdout or '') + ("\n" + stderr if stderr else '')
    # Define error and success heuristics
    error_keywords = [
        '失败', '错误', '异常', '未找到', 'Unknown', '未知', 'Traceback', 'Exception', 'Error', '终止', '无法', '参数不全', '语法错误'
    ]
    success_keywords = ['完成（dry-run）', '完成。', '完成。所有步骤均已按序执行']
    is_error = (returncode not in (0, None)) or any(kw in out for kw in error_keywords)
    is_success = any(kw in out for kw in success_keywords)
    # If neither error nor explicit success, but no obvious error markers, consider success
    if not is_error and (is_success or ('规划失败' not in out and '缺失参数' not in out)):
        return True, out
    return False, out


async def _ask_llm(session: aiohttp.ClientSession, background: str, prompt: str) -> str:
    resp = await call_llm_api(session, prompt=prompt, background=background)
    try:
        return resp["choices"][0]["message"]["content"]
    except Exception as e:
        raise RuntimeError(f"解析LLM响应失败: {e}; 原始: {resp}")


def _write_response_n(n: int, content: str) -> Path:
    RESPONSE_DIR.mkdir(parents=True, exist_ok=True)
    REPLIES_DIR.mkdir(parents=True, exist_ok=True)
    # save numbered
    p = REPLIES_DIR / f"response{n}.txt"
    p.write_text(content, encoding='utf-8')
    # mirror to response.txt for runner
    (RESPONSE_DIR / 'response.txt').write_text(content, encoding='utf-8')
    return p


def _write_parallel_artifacts(conv_id: int, round_id: int, *, ops_text: str, raw_reply: Optional[str] = None,
                               legal: Optional[bool] = None, errors: Optional[List[str]] = None,
                               attempt: int = 0) -> Dict[str, Path]:
    """Write per-conversation, per-round files into response/ for easier inspection.
    - response/response_m{conv}_r{round}[_a{attempt}].txt (sanitized ops)
    - response/m{conv}/response_r{round}[_a{attempt}].txt (same as above, per-conv folder)
    - response/runner_m{conv}_r{round}[_a{attempt}].log (legality result and messages)
    - response/raw_m{conv}_r{round}[_a{attempt}].txt (optional, raw reply)
    NOTE: DOES NOT overwrite response/response.txt to avoid confusion during parallel runs.
    """
    RESPONSE_DIR.mkdir(parents=True, exist_ok=True)
    REPLIES_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    files: Dict[str, Path] = {}
    # Flat file for quick grep
    suffix = f"_a{attempt}" if attempt else ""
    flat = REPLIES_DIR / f"response_m{conv_id}_r{round_id}{suffix}.txt"
    flat.write_text(ops_text, encoding='utf-8')
    files['response_flat'] = flat
    # Per-conversation folder
    conv_dir = REPLIES_DIR / f"m{conv_id}"
    conv_dir.mkdir(parents=True, exist_ok=True)
    per = conv_dir / f"response_r{round_id}{suffix}.txt"
    per.write_text(ops_text, encoding='utf-8')
    files['response_perconv'] = per
    # Raw reply (optional)
    if raw_reply is not None:
        rawp = REPLIES_DIR / f"raw_m{conv_id}_r{round_id}{suffix}.txt"
        rawp.write_text(raw_reply, encoding='utf-8')
        files['raw'] = rawp
    # Legality log
    logp = LOGS_DIR / f"runner_m{conv_id}_r{round_id}{suffix}.log"
    if legal is None:
        msg = "无校验结果"
    elif legal:
        msg = "合法性检查通过"
    else:
        msg = "合法性检查未通过\n" + "\n".join(errors or [])
    logp.write_text(msg, encoding='utf-8')
    files['log'] = logp
    return files


def _run_runner(args: argparse.Namespace) -> tuple[bool, str]:
    cmd = [sys.executable or 'python3', str(RUNNER)]
    # stats bundle meta.json (if provided)
    meta_from_stats = None
    try:
        if getattr(args, 'stats_dir', None):
            p = Path(args.stats_dir) / 'meta.json'
            if p.exists():
                meta_from_stats = str(p)
    except Exception:
        meta_from_stats = None
    if args.use_db:
        cmd.append('--use-db')
        cmd += ['--host', args.host, '--port', str(args.port), '--user', args.user, '--password', args.password]
        if args.database:
            cmd += ['--database', args.database]
    if args.sql_dir:
        cmd += ['--sql-dir', args.sql_dir]
    if args.out_sql_dir:
        cmd += ['--out-sql-dir', args.out_sql_dir]
    if getattr(args, 'schema_sql', None):
        cmd += ['--schema-sql', args.schema_sql]
    if getattr(args, 'stats_dir', None):
        cmd += ['--stats-dir', args.stats_dir]
    if getattr(args, 'storage_meta', None):
        cmd += ['--storage-meta', args.storage_meta]
    elif meta_from_stats:
        cmd += ['--storage-meta', meta_from_stats]
    if getattr(args, 'storage_budget', None):
        cmd += ['--storage-budget', args.storage_budget]
    # 集成 runner 的逐操作性能评估：当使用 --use-explain-debug（或明确提供 explain 目录）时自动启用
    try:
        enable_perf = bool(getattr(args, 'use_explain_debug', False)) or bool(getattr(args, 'explain_debug_dir', ''))
    except Exception:
        enable_perf = False
    if enable_perf:
        eval_explain_dir = getattr(args, 'explain_debug_dir', 'part2_debug') or 'part2_debug'
        eval_meta = getattr(args, 'storage_meta', 'output_dir/meta.json') or 'output_dir/meta.json'
        # 样本成本文件使用默认路径（由 performance_eval/extract_samples.py 生成）
        eval_samples = 'response/samples/samples.json'
        eval_out_json = str(RESPONSE_DIR / 'auto_eval_runner.json')
        eval_out_md = str(RESPONSE_DIR / 'auto_eval_runner.md')
        cmd += [
            '--eval-perf',
            '--eval-explain-dir', eval_explain_dir,
            '--eval-meta', eval_meta,
            '--eval-samples', eval_samples,
            '--eval-out-json', eval_out_json,
            '--eval-out-md', eval_out_md,
        ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    ok, out = _detect_success(proc.stdout, proc.stderr, proc.returncode)
    return ok, out


@dataclass
class Candidate:
    """Represents a single candidate operation sequence from a conversation round."""
    conv_id: int
    round_id: int
    ops_text: str
    legal: bool = False
    legal_errors: List[str] | None = None
    score_ms: Optional[int] = None  # smaller is better (total elapsed ms)
    details: Dict[str, Any] | None = None


def _import_runner_module():
    """Import response.runner as a module for in-process parsing/validation."""
    import importlib
    try:
        return importlib.import_module('response.runner')
    except Exception as e:  # pragma: no cover
        print(f"[auto-loop] 无法导入 response.runner: {e}")
        raise


def _schema_seed_from_file(schema_sql: Optional[str]) -> Optional[Dict[str, List[str]]]:
    """Load schema seed as table->columns mapping from a schema.sql file using prompt.PART2.parse_schema."""
    if not schema_sql:
        return None


def _schema_seed_from_stats(stats_dir: Optional[str]) -> Optional[Dict[str, List[str]]]:
    if not stats_dir:
        return None
    base = Path(stats_dir)
    if not base.exists():
        return None
    import json
    # Prefer table_columns.json, fallback to schema.json
    for name in ("table_columns.json", "schema.json"):
        p = base / name
        if not p.exists():
            continue
        try:
            data = json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        if name == "schema.json":
            # schema.json is table -> {col: type}
            return {t: list(cols.keys()) for t, cols in data.items() if isinstance(cols, dict)}
        # table_columns.json is table -> [cols]
        return {t: list(cols) for t, cols in data.items() if isinstance(cols, list)}
    return None
    try:
        import importlib
        P = importlib.import_module('prompt.PART2')
        tables = P.parse_schema(schema_sql)
        return {t: list(cols.keys()) for t, cols in tables.items()}
    except Exception as e:
        print(f"[auto-loop] 读取 schema 种子失败（将跳过顺序合法性校验的种子部分）：{e}")
        return None


def _check_ops_legality(ops_text: str, schema_seed: Optional[Dict[str, List[str]]] = None) -> Tuple[bool, List[str]]:
    """Parse ops and perform dependency-aware validation using runner internals.
    Returns (legal, errors).
    """
    runner_mod = _import_runner_module()
    try:
        parsed_ops = []
        for ln in [ln for ln in ops_text.splitlines() if ln.strip() and not ln.strip().startswith('#')]:
            parsed_ops.extend(runner_mod.parse_line_to_ops(ln))
        # Basic format check
        format_errs: List[str] = []
        for idx, po in enumerate(parsed_ops, 1):
            if po.kind in ('Unknown', 'ColumnSplit'):
                format_errs.append(f"格式错误：不支持的操作类型（OP #{idx}）：{po.raw}")
            if po.missing:
                format_errs.append(f"格式缺失：OP #{idx} {po.kind} -> {', '.join(po.missing)}")
        if format_errs:
            return False, format_errs
        # Sequential validation using virtual catalog if seed is provided
        seq_issues: List[str] = []
        if schema_seed is not None:
            seq_issues = runner_mod._sequential_validate(None, parsed_ops, schema_seed=schema_seed)  # type: ignore[attr-defined]
        if seq_issues:
            return False, seq_issues
        return True, []
    except Exception as e:
        return False, [f"合法性校验失败：{e}"]


def _split_sql_statements(sql_text: str) -> List[str]:
    """Very simple SQL splitter by semicolon, ignoring inside strings. Works for typical schema.sql."""
    stmts: List[str] = []
    cur = []
    in_s, in_d = False, False
    esc = False
    for ch in sql_text:
        cur.append(ch)
        if esc:
            esc = False
            continue
        if ch == '\\':
            esc = True
            continue
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        elif ch == ';' and not in_s and not in_d:
            piece = ''.join(cur[:-1]).strip()
            if piece:
                stmts.append(piece)
            cur = []
    tail = ''.join(cur).strip()
    if tail:
        stmts.append(tail)
    return stmts


def _read_ini_mysql(path: str) -> Dict[str, Any]:
    import configparser
    cfg = configparser.ConfigParser(interpolation=None, inline_comment_prefixes=())
    cfg.read(path)
    if 'mysql' not in cfg:
        raise RuntimeError(f"配置文件缺少 [mysql] 段：{path}")
    sec = cfg['mysql']
    def _opt(key, default=None):
        val = sec.get(key, fallback=default)
        if val is None:
            return None
        s = str(val).strip()
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            s = s[1:-1]
        return s
    return {
        'host': _opt('host', 'localhost'),
        'port': int(_opt('port', '3306')),
        'user': _opt('user', 'root'),
        'password': _opt('password', ''),
        'database': _opt('database', ''),
    }


def _eval_candidate_with_runner(
    candidate: Candidate,
    args: argparse.Namespace,
    eval_db_cfg: Dict[str, Any],
    schema_sql_path: Optional[str],
    base_sql_dir: Optional[str],
    out_base_dir: Path,
) -> Tuple[Optional[int], Dict[str, Any]]:
    """Apply candidate ops to a scratch DB, rewrite SQL, and measure latency.
    Returns (total_ms, details). None score means evaluation failed.
    """
    # Prepare response texts for runner
    REPLIES_DIR.mkdir(parents=True, exist_ok=True)
    resp_n = f"response_m{candidate.conv_id}_r{candidate.round_id}.txt"
    resp_path = REPLIES_DIR / resp_n
    resp_path.write_text(candidate.ops_text, encoding='utf-8')
    (RESPONSE_DIR / 'response.txt').write_text(candidate.ops_text, encoding='utf-8')

    # Scratch DB name
    base_db = eval_db_cfg.get('database') or 'tune_eval'
    suffix = int(time.time() * 1000) % 100000
    scratch_db = f"{base_db}_m{candidate.conv_id}r{candidate.round_id}_{suffix}"

    # Create/drop DB and load schema
    try:
        from DataBase.MySQLDriver import MySQLDriver
        # Step 1: connect without DB and (re)create scratch database
        cfg_server = dict(eval_db_cfg)
        cfg_server['database'] = None
        drv = MySQLDriver(cfg_server)
        if not drv.connect():
            return None, {'error': '无法连接评估数据库服务器'}
        drv.execute_statement(f"DROP DATABASE IF EXISTS `{scratch_db}`;")
        drv.execute_statement(f"CREATE DATABASE `{scratch_db}`;")
        drv.disconnect()

        # Step 2: connect to scratch DB and load schema SQL
        cfg_db = dict(eval_db_cfg)
        cfg_db['database'] = scratch_db
        drv2 = MySQLDriver(cfg_db)
        if not drv2.connect():
            return None, {'error': f'无法连接到临时数据库 {scratch_db}'}
        if schema_sql_path and os.path.exists(schema_sql_path):
            sql_text = Path(schema_sql_path).read_text(encoding='utf-8')
            for stmt in _split_sql_statements(sql_text):
                ok = drv2.execute_statement(stmt)
                if not ok:
                    try:
                        drv2.disconnect()
                    except Exception:
                        pass
                    return None, {'error': f'加载 schema 失败: {stmt[:80]}...'}
        # Keep connection for query_latency runs later
        try:
            drv2.disconnect()
        except Exception:
            pass
    except Exception as e:
        return None, {'error': f'准备评估数据库失败: {e}'}

    # Step 3: run runner to apply ops and rewrite SQL into out_dir
    eval_out_dir = out_base_dir / f"m{candidate.conv_id}_r{candidate.round_id}"
    eval_out_dir.parent.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable or 'python3', str(RUNNER), '--use-db',
           '--host', str(eval_db_cfg['host']), '--port', str(eval_db_cfg['port']),
           '--user', str(eval_db_cfg['user']), '--password', str(eval_db_cfg.get('password') or ''),
           '--database', scratch_db]
    if base_sql_dir:
        cmd += ['--sql-dir', base_sql_dir, '--out-sql-dir', str(eval_out_dir)]
    if schema_sql_path:
        cmd += ['--schema-sql', schema_sql_path]
    if getattr(args, 'stats_dir', None):
        cmd += ['--stats-dir', args.stats_dir]
    # Intentionally rely on runner default to apply schema when --use-db provided
    proc = subprocess.run(cmd, capture_output=True, text=True)
    ok, out = _detect_success(proc.stdout, proc.stderr, proc.returncode)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    (LOGS_DIR / f"runner_eval_m{candidate.conv_id}_r{candidate.round_id}.log").write_text(out, encoding='utf-8')
    if not ok:
        return None, {'error': 'runner 执行失败，请检查日志'}

    # Step 4: measure latency using query_latency/collect_latency.py
    total_ms: Optional[int] = None
    details: Dict[str, Any] = {}
    if base_sql_dir:
        # produce a temporary INI config targeting scratch_db
        ini_path = RESPONSE_DIR / f"eval_mysql_m{candidate.conv_id}_r{candidate.round_id}.ini"
        ini_text = (
            "[mysql]\n"
            f"host={eval_db_cfg['host']}\n"
            f"port={eval_db_cfg['port']}\n"
            f"user={eval_db_cfg['user']}\n"
            f"password={eval_db_cfg.get('password') or ''}\n"
            f"database={scratch_db}\n"
        )
        ini_path.write_text(ini_text, encoding='utf-8')
        out_csv = LOGS_DIR / f"latency_m{candidate.conv_id}_r{candidate.round_id}.csv"
        err_csv = LOGS_DIR / f"latency_err_m{candidate.conv_id}_r{candidate.round_id}.csv"
        cmd2 = [sys.executable or 'python3', str(ROOT / 'query_latency' / 'collect_latency.py'),
                '--sql-dir', str(eval_out_dir), '--config', str(ini_path),
                '--output', str(out_csv), '--error-output', str(err_csv)]
        proc2 = subprocess.run(cmd2, capture_output=True, text=True)
        # Parse output CSV
        if out_csv.exists():
            try:
                import csv
                rows = []
                with out_csv.open('r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        try:
                            ms = int(row.get('elapsed_ms') or row.get('avg_elapsed_ms') or '0')
                        except Exception:
                            ms = 0
                        rows.append(ms)
                total_ms = sum(rows) if rows else None
                details.update({'csv_rows': len(rows)})
            except Exception:
                total_ms = None
        details.update({'collect_stdout': proc2.stdout[-5000:], 'collect_stderr': proc2.stderr[-5000:]})

    # Step 5: cleanup scratch db (best-effort)
    try:
        from DataBase.MySQLDriver import MySQLDriver
        cfg_server = dict(eval_db_cfg)
        cfg_server['database'] = None
        drv = MySQLDriver(cfg_server)
        if drv.connect():
            drv.execute_statement(f"DROP DATABASE IF EXISTS `{scratch_db}`;")
            drv.disconnect()
    except Exception:
        pass

    return total_ms, details


def _build_part2_mapping(schema_sql: str, sql_dir: str, eval_db_cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Invoke prompt.PART2.build_part2 and return parsed mapping (dict) or None on failure."""
    try:
        import importlib, json
        P2 = importlib.import_module('prompt.PART2')
        txt = P2.build_part2(
            schema_sql_path=schema_sql,
            sql_dir=sql_dir,
            dialect='mysql',
            host=str(eval_db_cfg.get('host', '127.0.0.1')),
            port=int(eval_db_cfg.get('port', 3306)),
            user=str(eval_db_cfg.get('user', 'root')),
            password=str(eval_db_cfg.get('password', '')),
            database=str(eval_db_cfg.get('database', '')),
            config_path=str(ROOT / 'query_latency' / 'db_config.ini'),
            debug=False,
            debug_dir=str(ROOT / 'debug' / 'part2'),
            exec_counts_path=str(ROOT / 'Data' / 'cleaned_sql' / 'query_and_update' / 'sample_execution_counts_chbench.csv'),
        )
        return json.loads(txt)
    except Exception as e:
        print(f"[auto-loop] 构建 PART2 映射失败：{e}")
        return None


def _per_table_row_bytes(meta: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for t, td in (meta.get('tables') or {}).items():
        per = 0.0
        for cn, cd in (td.get('columns') or {}).items():
            try:
                per += float(cd.get('avg_length', 0.0)) * (1.0 - float(cd.get('null_frac', 0.0)))
            except Exception:
                pass
        out[str(t)] = float(per)
    return out


def _static_cost_eval(
    baseline_map: Dict[str, Any],
    init_meta: Dict[str, Any],
    ops_text: str,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """Static proportional cost reweighting based on storage meta changes.
    Returns (pred_total_cost_ms, details). Lower is better.
    """
    # 1) Build base per-table costs by op type
    base_scan: Dict[str, float] = {}
    base_join: Dict[str, float] = {}
    base_grp: Dict[str, float] = {}
    base_ord: Dict[str, float] = {}
    table_rows: Dict[str, int] = {}
    table_neighbors: Dict[str, Set[str]] = {}

    for t, cols in baseline_map.items():
        if not isinstance(cols, dict):
            continue
        table_rows[str(t)] = int(cols.get('表行数', 0) or 0)
        # neighbors for join scaling
        neigh = set()
        arr = cols.get('join')
        if isinstance(arr, list):
            for it in arr:
                other = str((it or {}).get('table', ''))
                if other:
                    neigh.add(other)
        table_neighbors[str(t)] = neigh
        # per-column items
        for c, items in cols.items():
            if not isinstance(items, list):
                continue
            for it in items:
                try:
                    op = str(it.get('operation', ''))
                except Exception:
                    continue
                # 优先使用 PART2 的 EXPLAIN 归因总时间（毫秒）
                cost = it.get('sum_time_ms')
                if cost is None:
                    # 兼容旧字段
                    cost = it.get('cost')
                if cost is None:
                    try:
                        cost = float(it.get('avg_time', 0.0)) * float(it.get('count', 0))
                    except Exception:
                        cost = 0.0
                cost = float(cost or 0.0)
                if op == 'scan':
                    base_scan[str(t)] = base_scan.get(str(t), 0.0) + cost
                elif op.startswith('join('):
                    base_join[str(t)] = base_join.get(str(t), 0.0) + cost
                elif op == 'group by':
                    base_grp[str(t)] = base_grp.get(str(t), 0.0) + cost
                elif op == 'order by':
                    base_ord[str(t)] = base_ord.get(str(t), 0.0) + cost

    base_total = sum(base_scan.values()) + sum(base_join.values()) + sum(base_grp.values()) + sum(base_ord.values())
    if base_total <= 0:
        return None, {'error': 'baseline cost is zero; 无法进行比例评估'}

    # 2) Apply ops to storage meta
    try:
        import json
        from scripts.storage_transformer import StorageModel  # type: ignore
        model = StorageModel(init_meta)
        # Convert ops_text to list of operations: supports '-' chain and newline separated
        all_ops: List[str] = []
        for line in [ln.strip() for ln in ops_text.splitlines() if ln.strip()]:
            # split by '-' at top level
            tmp = []
            depth = 0
            start = 0
            s = line
            for i, ch in enumerate(s):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth = max(0, depth - 1)
                elif ch == '-' and depth == 0:
                    piece = s[start:i].strip().rstrip(';')
                    if piece:
                        tmp.append(piece)
                    start = i + 1
            last = s[start:].strip().rstrip(';')
            if last:
                tmp.append(last)
            all_ops.extend(tmp)
        for op in all_ops:
            model.apply(op)
        new_meta = model.meta
    except Exception as e:
        return None, {'error': f'应用操作到存储模型失败：{e}'}

    # 3) Build factors
    old_row_bytes = _per_table_row_bytes(init_meta)
    new_row_bytes = _per_table_row_bytes(new_meta)
    def _fw(t: str) -> float:
        a = float(new_row_bytes.get(t, 0.0))
        b = float(old_row_bytes.get(t, 0.0))
        if b <= 0:
            return 0.0 if a <= 0 else 1.0
        return max(0.0, a / b)
    def _fr(t: str) -> float:
        a = int((new_meta.get('tables', {}).get(t, {}) or {}).get('row_count', 0) or 0)
        b = int((init_meta.get('tables', {}).get(t, {}) or {}).get('row_count', 0) or 0)
        if b <= 0:
            return 0.0 if a <= 0 else 1.0
        return max(0.0, float(a) / float(b))

    # 4) Reweight costs per table and aggregate
    pred_total = 0.0
    breakdown: Dict[str, Dict[str, float]] = {}
    for t in set(list(base_scan.keys()) + list(base_join.keys()) + list(base_grp.keys()) + list(base_ord.keys())):
        fw = _fw(t)
        fr = _fr(t)
        # neighbor factor for joins
        neigh = list(table_neighbors.get(t, set()))
        if neigh:
            nf = 0.0
            for u in neigh:
                nf += (_fw(u) * _fr(u))
            nf = nf / float(len(neigh))
        else:
            nf = 1.0

        scan_new = base_scan.get(t, 0.0) * fw * fr
        # simple mean of sides to scale join contribution for t
        join_new = base_join.get(t, 0.0) * ((fw * fr + nf) / 2.0)
        # N log N approx for sort/group
        oldN = float(max(2, table_rows.get(t, 1)))
        newN = max(2.0, oldN * fr)
        log_ratio = (float((newN).bit_length()) / float((oldN).bit_length())) if isinstance(newN, int) and isinstance(oldN, int) else (float((newN)) / float((oldN)))**0.0  # fallback ~1.0
        try:
            import math
            log_ratio = math.log2(newN) / math.log2(oldN)
        except Exception:
            log_ratio = 1.0
        grp_new = base_grp.get(t, 0.0) * fw * fr * log_ratio
        ord_new = base_ord.get(t, 0.0) * fw * fr * log_ratio
        total_t = scan_new + join_new + grp_new + ord_new
        pred_total += total_t
        breakdown[t] = {
            'scan': scan_new,
            'join': join_new,
            'group': grp_new,
            'order': ord_new,
            'factor_rows': fr,
            'factor_width': fw,
        }

    details = {
        'baseline_total_ms': base_total,
        'pred_total_ms': pred_total,
        'delta_ms': pred_total - base_total,
        'improvement_ms': base_total - pred_total,
        'breakdown': breakdown,
    }
    return pred_total, details


# --- Pure static evaluator based on SQL replacement mapping (no DB) ---
def _static_cost_eval_sqlmap(
    baseline_map: Dict[str, Any],
    ops_text: str,
) -> Tuple[Optional[float], Dict[str, Any]]:
    """Estimate latency change by eliminating join costs for table pairs that are
    materialized via TableJoin, without touching DB or storage meta.

    Algorithm:
    - Baseline total: sum of per-column sum_time_ms for operations in {scan, join(*), group by, order by}.
    - Build per-pair join contributions J[(a,b)] from baseline_map by matching join(col) items
      against PART2's table-level join pairs metadata.
    - Parse ops_text; for each TableJoin(A,B, ...) consider pair {A,B} eliminated and subtract J[(a,b)].
    - Other ops (VerticalSplit/HorizontalSplit/Merge/Redundant*) do not change cost here.
    Returns (pred_total_ms, details).
    """
    import re
    # 1) Baseline total and collect per-table per-column items
    base_total = 0.0
    # Build quick lookup: table -> column -> list[ {operation,sum_time_ms} ]
    tbl_col_items: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for t, cols in (baseline_map or {}).items():
        if not isinstance(cols, dict):
            continue
        for c, items in cols.items():
            if c == 'join':
                continue
            if not isinstance(items, list):
                continue
            for it in items:
                op = str(it.get('operation', '')).strip()
                cost = it.get('sum_time_ms')
                if cost is None:
                    try:
                        cost = float(it.get('avg_time', 0.0)) * float(it.get('count', 0))
                    except Exception:
                        cost = 0.0
                cost = float(cost or 0.0)
                if op == 'scan' or op == 'group by' or op == 'order by' or op.startswith('join('):
                    base_total += cost
                tbl_col_items.setdefault(str(t), {}).setdefault(str(c), []).append({'operation': op, 'sum_time_ms': cost})

    # 2) Build per-pair join contribution J[(a,b)] using PART2 table-level join pairs
    #    result[table]['join'] = [{'table': other, 'count': N, 'pairs': [[col, other_col], ...]}, ...]
    def _pairs_for(t: str) -> Dict[str, List[Tuple[str, str]]]:
        out: Dict[str, List[Tuple[str, str]]] = {}
        arr = (baseline_map.get(t, {}) or {}).get('join')
        if isinstance(arr, list):
            for entry in arr:
                try:
                    other = str((entry or {}).get('table', ''))
                    plist = []
                    for pr in (entry or {}).get('pairs') or []:
                        a = pr[0] if isinstance(pr, (list, tuple)) and len(pr) >= 1 else None
                        b = pr[1] if isinstance(pr, (list, tuple)) and len(pr) >= 2 else None
                        if isinstance(a, str) and isinstance(b, str):
                            plist.append((a, b))
                    if other and plist:
                        out[other] = plist
                except Exception:
                    continue
        return out

    J: Dict[Tuple[str, str], float] = {}
    # Helper to add symmetric pair key
    def _add_pair(a: str, b: str, v: float) -> None:
        if not a or not b or v <= 0:
            return
        x, y = sorted([a, b])
        J[(x, y)] = J.get((x, y), 0.0) + float(v)

    # For each table and column-level join(op), attribute to concrete neighbor(s)
    for t, col_map in tbl_col_items.items():
        pair_map = _pairs_for(t)  # other -> [(col, other_col)]
        # Precompute reverse index: for a column c, map other table by matching (c, other_col)
        idx: Dict[str, List[Tuple[str, str]]] = {}
        for other, plist in pair_map.items():
            for (c, oc) in plist:
                idx.setdefault(c, []).append((other, oc))
        for c, items in col_map.items():
            # collect this column's join ops
            for it in items:
                op = it.get('operation', '')
                if not (isinstance(op, str) and op.startswith('join(')):
                    continue
                cost = float(it.get('sum_time_ms') or 0.0)
                if cost <= 0:
                    continue
                m = re.match(r"join\(([^)]+)\)", op)
                other_col = m.group(1) if m else None
                cand = [(o, oc) for (o, oc) in idx.get(str(c), []) if (other_col is None or str(oc) == str(other_col))]
                if not cand:
                    # Fallback: attribute to all neighbors of this column equally (rare ambiguous case)
                    cand = idx.get(str(c), [])
                if cand:
                    w = 1.0 / float(len(cand))
                    for (other, _oc) in cand:
                        _add_pair(t, other, cost * w)

    # 3) Parse TableJoin ops and compute eliminated join ms
    eliminated_pairs: Set[Tuple[str, str]] = set()
    for line in [ln.strip() for ln in (ops_text or '').splitlines() if ln.strip()]:
        try:
            # split top-level '-' chains
            s = line
            depth = 0
            start = 0
            parts: List[str] = []
            for i, ch in enumerate(s):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth = max(0, depth - 1)
                elif ch == '-' and depth == 0:
                    token = s[start:i].strip().rstrip(';')
                    if token:
                        parts.append(token)
                    start = i + 1
            tail = s[start:].strip().rstrip(';')
            if tail:
                parts.append(tail)
        except Exception:
            parts = [line]
        for op in parts:
            if not op.startswith('TableJoin('):
                continue
            m = re.match(r"^TableJoin\(([^)]*)\)", op)
            if not m:
                continue
            args = m.group(1)
            # args: T1, T2, ...
            toks: List[str] = []
            buf, d = [], 0
            for ch in args:
                if ch == '(':
                    d += 1
                elif ch == ')':
                    d = max(0, d - 1)
                elif ch == ',' and d == 0:
                    token = ''.join(buf).strip()
                    if token:
                        toks.append(token)
                    buf = []
                    continue
                buf.append(ch)
            last = ''.join(buf).strip()
            if last:
                toks.append(last)
            if len(toks) >= 2:
                a = toks[0].split('.')[-1].strip('`" ')
                b = toks[1].split('.')[-1].strip('`" ')
                x, y = sorted([a, b])
                eliminated_pairs.add((x, y))

    eliminated_ms = 0.0
    eliminated_detail: Dict[str, float] = {}
    for pair, v in J.items():
        if pair in eliminated_pairs:
            eliminated_ms += v
            eliminated_detail[f"{pair[0]}+{pair[1]}"] = float(v)

    pred_total = max(0.0, base_total - eliminated_ms)
    details = {
        'baseline_total_ms': base_total,
        'pred_total_ms': pred_total,
        'delta_ms': pred_total - base_total,
        'improvement_ms': base_total - pred_total,
        'eliminated_join_pairs_ms': eliminated_detail,
    }
    return pred_total, details


def _static_cost_eval_explain(
    debug_dir: str,
    sql_dir: str,
    ops_text: str,
    init_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[float], Dict[str, Any]]:
    import json
    from performance_eval.plan import parse_plan, compute_total_cost
    try:
        from analysis import sql_usage_diff as sud
    except Exception:
        sud = None  # type: ignore

    base = Path(debug_dir)
    idx_path = base / 'index_map.json'
    explain_dir = base / 'explain'
    sql_map_dir = base / 'sql'
    if not idx_path.exists() or not explain_dir.exists() or not sql_map_dir.exists():
        return None, {'error': f'debug 目录缺少必要文件：{idx_path}, {explain_dir}, {sql_map_dir}'}
    index_map = json.loads(idx_path.read_text(encoding='utf-8'))

    # Ops mapping (merge/split)
    try:
        merge_map, split_map = sud._build_table_mapping(ops_text) if sud else ({}, {})
    except Exception:
        merge_map, split_map = {}, {}

    # Storage meta scaling
    old_row_bytes: Dict[str, float] = {}
    new_row_bytes: Dict[str, float] = {}
    old_rows: Dict[str, int] = {}
    new_rows: Dict[str, int] = {}
    if init_meta is not None:
        try:
            from scripts.storage_transformer import StorageModel  # type: ignore
            old_row_bytes = _per_table_row_bytes(init_meta)
            for t, td in (init_meta.get('tables') or {}).items():
                old_rows[str(t)] = int(td.get('row_count') or 0)
            model = StorageModel(init_meta)
            # Feed ops sequentially
            for ln in [ln.strip() for ln in (ops_text or '').splitlines() if ln.strip()]:
                s, depth, start = ln, 0, 0
                for i, ch in enumerate(s):
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth = max(0, depth - 1)
                    elif ch == '-' and depth == 0:
                        token = s[start:i].strip().rstrip(';')
                        if token:
                            try:
                                model.apply(token)
                            except Exception:
                                pass
                        start = i + 1
                tail = s[start:].strip().rstrip(';')
                if tail:
                    try:
                        model.apply(tail)
                    except Exception:
                        pass
            new_meta = model.meta
            new_row_bytes = _per_table_row_bytes(new_meta)
            for t, td in (new_meta.get('tables') or {}).items():
                new_rows[str(t)] = int(td.get('row_count') or 0)
        except Exception:
            init_meta = None

    def _pair_key(a: str, b: str) -> Tuple[str, str]:
        x, y = sorted([a, b])
        return (x, y)

    def _map_old_to_new(t: str) -> str:
        return merge_map.get(t, t)

    def _fw_fr(old_t: str, new_t: str) -> float:
        if init_meta is None:
            return 1.0
        a = float(new_row_bytes.get(new_t, 0.0))
        b = float(old_row_bytes.get(old_t, 0.0))
        fw = (a / b) if b > 0 else 1.0
        ra = int(new_rows.get(new_t, 0) or 0)
        rb = int(old_rows.get(old_t, 0) or 0)
        fr = (float(ra) / float(rb)) if rb > 0 else 1.0
        return max(0.0, fw * fr)

    total_base_s = 0.0
    total_pred_s = 0.0
    per_query: List[Dict[str, Any]] = []

    for ent in index_map:
        qid = int(ent.get('id'))
        exp_path = explain_dir / f"q{qid}.txt"
        sql_path = sql_map_dir / f"q{qid}.sql"
        if not exp_path.exists() or not sql_path.exists():
            continue
        try:
            exp_text = exp_path.read_text(encoding='utf-8', errors='ignore')
            nodes = parse_plan(exp_text)
            base_s = compute_total_cost(nodes)
            # Aggregate join and scan costs
            join_like_s = 0.0
            scan_by_table: Dict[str, float] = {}
            for n in nodes:
                c = float(n.cost) if n.cost is not None else 0.0
                if n.type in ('hash_join', 'nested_loop'):
                    join_like_s += c
                elif n.type in ('table_scan', 'index_scan', 'index_lookup', 'single_row_lookup'):
                    if n.tables:
                        t0 = n.tables[0]
                        scan_by_table[t0] = scan_by_table.get(t0, 0.0) + c

            sql_orig = sql_path.read_text(encoding='utf-8', errors='ignore')
            tables_orig = sud._extract_tables(sql_orig) if (sud and sql_orig) else set()
            pairs_old = sud._extract_join_pairs(sql_orig) if (sud and sql_orig) else set()
            pairs_new = sud.remap_join_pairs(pairs_old, merge_map, split_map) if sud else set()
            pairs_old = set(_pair_key(a, b) for (a, b) in pairs_old)
            pairs_new = set(_pair_key(a, b) for (a, b) in pairs_new)
            elim_pairs = pairs_old - pairs_new if pairs_old else set()
            elim_frac = (float(len(elim_pairs)) / float(len(pairs_old))) if pairs_old else 0.0

            sum_scan_base = sum(scan_by_table.values()) if scan_by_table else 0.0
            sum_scan_scaled = 0.0
            # Only remap scans for tables that participate in eliminated pairs
            involved: Set[str] = set()
            for a, b in elim_pairs:
                involved.add(a); involved.add(b)
            for t, base_scan in scan_by_table.items():
                new_t = _map_old_to_new(t) if t in involved else t
                sum_scan_scaled += base_scan * _fw_fr(t, new_t)

            other_s = max(0.0, base_s - join_like_s - sum_scan_base)
            if init_meta is not None and tables_orig:
                frs: List[float] = []
                for t in tables_orig:
                    t2 = _map_old_to_new(t) if t in involved else t
                    ra = int(new_rows.get(t2, 0) or 0)
                    rb = int(old_rows.get(t, 0) or 0)
                    if rb > 0:
                        frs.append(float(ra) / float(rb))
                other_factor = (sum(frs) / float(len(frs))) if frs else 1.0
            else:
                other_factor = 1.0

            pred_s = other_s * other_factor + sum_scan_scaled + join_like_s * (1.0 - elim_frac)
            total_base_s += base_s
            total_pred_s += pred_s
            per_query.append({
                'qid': qid,
                'baseline_s': base_s,
                'pred_s': pred_s,
                'join_like_s': join_like_s,
                'scan_by_table': scan_by_table,
                'tables_orig': sorted(list(tables_orig)) if tables_orig else [],
                'pairs_old': sorted([list(p) for p in pairs_old]) if pairs_old else [],
                'pairs_new': sorted([list(p) for p in pairs_new]) if pairs_new else [],
                'elim_pairs': sorted([list(p) for p in elim_pairs]) if elim_pairs else [],
                'elim_frac': elim_frac,
                'scan_scaled_ms': sum_scan_scaled * 1000.0,
                'other_scaled_ms': other_s * other_factor * 1000.0,
            })
        except Exception as e:
            per_query.append({'qid': qid, 'error': str(e)})
            continue

    details = {
        'baseline_total_cost': total_base_s,
        'pred_total_cost': total_pred_s,
        'delta_cost': (total_pred_s - total_base_s),
        'improvement_cost': (total_base_s - total_pred_s),
        'per_query': per_query,
        'source': 'explain_debug(cost)',
        'debug_dir': str(debug_dir),
    }
    return details['pred_total_cost'], details
# 全局禁用候选序列的“动态（连库实测）评估”
DISABLE_DYNAMIC_EVAL = True

async def main_async(args: argparse.Namespace) -> int:
    # 1) Initial prompt
    init_text, used_file = _read_initial_prompt()
    print(f"[auto-loop] 使用初始提示文件: {used_file}")

    # Advanced parallel selection mode
    if args.parallel_m > 0:
        print(
            f"[auto-loop] 启用并行模式：m={args.parallel_m}, n={args.rounds_n}, k={args.select_k}, s={args.opt_rounds_s}"
        )
        if DISABLE_DYNAMIC_EVAL:
            print("[auto-loop] 已全局禁用动态评估（不连库实测）；仅使用静态/EXPLAIN 离线评估。")
        schema_seed = _schema_seed_from_stats(getattr(args, 'stats_dir', None)) or _schema_seed_from_file(args.schema_sql)
        leaderboard: List[Candidate] = []

        async with aiohttp.ClientSession() as session:
            # Conversation state
            prompts: List[Tuple[str, str]] = [(init_text, init_text) for _ in range(args.parallel_m)]
            obtained: List[Optional[Candidate]] = [None for _ in range(args.parallel_m)]  # first legal per conv

            # Optional baseline for static eval and storage meta
            baseline_map = None
            init_meta = None
            # Pure-static mode: prefer precomputed PART2 mapping if provided
            if args.eval_mode in ('static', 'both') and args.eval_sql_dir and args.schema_sql:
                # 1) Try explicit baseline JSON (no DB)
                if getattr(args, 'baseline_json', None):
                    try:
                        import json
                        with open(args.baseline_json, 'r', encoding='utf-8') as f:
                            baseline_map = json.load(f)
                    except Exception as e:
                        print(f"[auto-loop] 读取 baseline-json 失败：{e}")
                # 2)（禁用）DB 构建 PART2 基线映射：遵循“评估不连库”原则，永远跳过
                if baseline_map is None and not getattr(args, 'no_db_eval', False) and args.eval_db_config:
                    if not DISABLE_DYNAMIC_EVAL:
                        try:
                            eval_db_cfg0 = _read_ini_mysql(args.eval_db_config)
                            baseline_map = _build_part2_mapping(args.schema_sql, args.eval_sql_dir, eval_db_cfg0)
                        except Exception:
                            baseline_map = None
                    else:
                        print("[auto-loop] 跳过基线映射的 DB 构建（动态评估已禁用）。")
                # Storage meta only needed for legacy storage-based static eval
                try:
                    import json
                    if args.storage_meta:
                        with open(args.storage_meta, 'r', encoding='utf-8') as f:
                            init_meta = json.load(f)
                except Exception:
                    init_meta = None

            for r in range(1, args.rounds_n + 1):
                print(f"\n[auto-loop] 第 {r} 轮：并行请求 {args.parallel_m} 个对话 ...")
                tasks = []
                idx_map = []  # map of task index to conv index
                for j in range(args.parallel_m):
                    # Skip conversations that already have a legal sequence
                    if obtained[j] is not None:
                        continue
                    bg, pr = prompts[j]
                    if r > 1:
                        bg = init_text
                    tasks.append(_ask_llm(session, background=bg, prompt=pr))
                    idx_map.append(j)
                # If all conversations already obtained legal, stop early
                if not tasks:
                    break
                replies = await asyncio.gather(*tasks, return_exceptions=True)

                for t_idx, rep in enumerate(replies):
                    j = idx_map[t_idx]
                    if isinstance(rep, Exception):
                        print(f"[auto-loop] 对话#{j+1} 调用失败：{rep}")
                        continue
                    raw_reply = str(rep)
                    ops_text = _sanitize_llm_reply_to_ops(raw_reply)
                    legal, errs = _check_ops_legality(ops_text, schema_seed=schema_seed)
                    _write_parallel_artifacts(j + 1, r, ops_text=ops_text, raw_reply=raw_reply, legal=legal, errors=errs)
                    if legal and obtained[j] is None:
                        obtained[j] = Candidate(conv_id=j + 1, round_id=r, ops_text=ops_text, legal=True)
                        # Freeze this conversation; do not request more rounds for it
                        print(f"[auto-loop] 对话#{j+1} 在第 {r} 轮通过合法性校验，等待其他对话结束 …")
                        continue
                    # Not legal → prepare repair prompt for next round
                    err_snippet = "\n".join(errs or [])[:4000]
                    # 离线性能反馈（算子级摘要 + DML+runner逐步），仅在启用 explain-debug 时构造
                    perf_fb = ''
                    try:
                        if args.use_explain_debug and args.explain_debug_dir:
                            auto_pats = _infer_prune_patterns_from_ops(ops_text)
                            perf_fb, _ = _build_perf_feedback(
                                ops_text,
                                explain_dir=args.explain_debug_dir,
                                storage_meta_path=getattr(args, 'storage_meta', None),
                                prune_patterns=auto_pats,
                            )
                            # 若 runner 端已生成逐操作评估文件，则将其附加到反馈中，帮助 LLM 直接看到每步增减
                            try:
                                md_path = RESPONSE_DIR / 'auto_eval_runner.md'
                                if md_path.exists():
                                    fb2 = md_path.read_text(encoding='utf-8')
                                    perf_fb = (perf_fb + "\n\n" + fb2).strip()
                            except Exception:
                                pass
                        if perf_fb:
                            # 写入专用目录 response/perf_fb/
                            try:
                                PERF_FB_DIR.mkdir(parents=True, exist_ok=True)
                            except Exception:
                                pass
                            (PERF_FB_DIR / f'perf_fb_m{j+1}_r{r}.md').write_text(perf_fb, encoding='utf-8')
                    except Exception:
                        perf_fb = ''

                    repair_prompt = (
                        "请基于上一次给出的操作序列进行修正，并继续使用相同的输出格式：\n"
                        "- 只输出操作序列（不含解释、不含其他文字）\n"
                        "- 使用短横线 '-' 串联多个操作，或一行一个操作\n"
                        "- 使用以下支持的操作：VerticalSplit / HorizontalSplit / HorizontalMerge / TableJoin / RedundantColumnAdd / RedundantColumnDrop\n\n"
                        f"上一轮操作序列：\n{ops_text}\n\n"
                        f"约束/语义校验错误：\n{err_snippet}\n\n"
                        f"性能评估反馈（离线+runner逐步）：\n{perf_fb}\n\n"
                        "请给出新的操作序列以修复上述问题。"
                    )
                    prompts[j] = (init_text, repair_prompt)

            # Gather all legal candidates after up to n rounds
            legal_set = [c for c in obtained if c is not None]
            if not legal_set:
                (RESPONSE_DIR / 'failure.txt').write_text(
                    "并行阶段未找到任何对话的合法候选，请检查 response/runner_m*_r*.log 与 response/response_m*_r*.txt。\n",
                    encoding='utf-8',
                )
                print("\n[auto-loop] 结束：未获取到合法候选。")
                return 3

            # Evaluate all legal candidates together, then pick top-k
            def _score_key(c: Candidate):
                return c.score_ms if isinstance(c.score_ms, int) else sys.maxsize
            eval_db_cfg: Optional[Dict[str, Any]] = None
            if args.eval_db_config and not getattr(args, 'no_db_eval', False):
                try:
                    eval_db_cfg = _read_ini_mysql(args.eval_db_config)
                except Exception as e:
                    print(f"[auto-loop] 读取评估 DB 配置失败：{e}")
            out_base = ROOT / 'output_dir' / 'eval_out'
            for cand in legal_set:
                det_all: Dict[str, Any] = {}
                if args.eval_mode in ('static', 'both') and baseline_map:
                    # Prefer SQL-mapping based estimator to avoid DB assumptions
                    try:
                        s_score, s_det = _static_cost_eval_sqlmap(baseline_map, cand.ops_text)
                    except Exception:
                        s_score, s_det = _static_cost_eval(baseline_map, init_meta or {}, cand.ops_text) if init_meta else (None, {'error': 'no init_meta for legacy static eval'})
                    if s_score is not None:
                        cand.score_ms = int(s_score)
                    det_all['static'] = s_det
                    try:
                        import json
                        (RESPONSE_DIR / f"eval_static_m{cand.conv_id}_r{cand.round_id}.json").write_text(
                            json.dumps({'score_ms': s_score, 'details': s_det}, ensure_ascii=False, indent=2),
                            encoding='utf-8')
                    except Exception:
                        pass
                if args.eval_mode in ('dynamic', 'both') and (not getattr(args, 'no_db_eval', False)) and eval_db_cfg and args.eval_sql_dir:
                    d_score, d_det = _eval_candidate_with_runner(
                        cand, args, eval_db_cfg, args.schema_sql, args.eval_sql_dir, out_base,
                    )
                    if d_score is not None:
                        cand.score_ms = int(d_score)
                    det_all['dynamic'] = d_det
                # Offline EXPLAIN-based static (if debug dir present) and no DB
                if args.eval_mode in ('static', 'both') and getattr(args, 'use_explain_debug', False) and getattr(args, 'no_db_eval', False):
                    ex_score, ex_det = _static_cost_eval_explain(
                        getattr(args, 'explain_debug_dir'), args.eval_sql_dir, cand.ops_text,
                    )
                    if ex_score is not None:
                        cand.score_ms = int(ex_score)
                    det_all['static_explain'] = ex_det
                cand.details = det_all or {'note': '未提供评估环境，暂不计算性能得分'}
            # sort ascending score (ms)
            legal_set.sort(key=_score_key)
            finalists = legal_set[: max(1, args.select_k or 1)]

            # For each finalist, run s optimization rounds. In each round, query until get a legal refined sequence, then evaluate.
            async def _optimize_candidate(base: Candidate, rank: int) -> Candidate:
                best = Candidate(conv_id=base.conv_id, round_id=base.round_id, ops_text=base.ops_text, legal=True, score_ms=base.score_ms, details=base.details)
                if args.opt_rounds_s <= 0:
                    return best
                max_attempts = max(1, args.max_iters)  # cap for retries per opt round
                for s_idx in range(1, args.opt_rounds_s + 1):
                    print(f"\n[auto-loop] Finalist#{rank} 优化轮 #{s_idx} …")
                    attempt = 0
                    refined_ops = None
                    while attempt < max_attempts:
                        attempt += 1
                        refined = await _ask_llm(
                            session,
                            background=init_text,
                            prompt=(
                                "在保持语义正确的前提下，优化下述操作序列以进一步降低查询延迟与存储成本。\n"
                                "- 只输出操作序列本身（不附带解释）\n\n"
                                f"当前操作序列：\n{best.ops_text}\n"
                            ),
                        )
                        raw_reply = str(refined)
                        ops = _sanitize_llm_reply_to_ops(raw_reply)
                        legal, errs = _check_ops_legality(ops, schema_seed=schema_seed)
                        _write_parallel_artifacts(base.conv_id, base.round_id + s_idx, ops_text=ops, raw_reply=raw_reply, legal=legal, errors=errs, attempt=attempt)
                        if legal:
                            refined_ops = ops
                            break
                    if refined_ops is None:
                        print(f"[auto-loop] Finalist#{rank} 优化轮 #{s_idx} 未获得合法序列，跳过")
                        continue
                    # evaluate refined_ops
                    tmp = Candidate(conv_id=base.conv_id, round_id=base.round_id + s_idx, ops_text=refined_ops, legal=True)
                    det_all: Dict[str, Any] = {}
                    if args.eval_mode in ('static', 'both') and baseline_map:
                        try:
                            s_score, s_det = _static_cost_eval_sqlmap(baseline_map, tmp.ops_text)
                        except Exception:
                            s_score, s_det = _static_cost_eval(baseline_map, init_meta or {}, tmp.ops_text) if init_meta else (None, {'error': 'no init_meta for legacy static eval'})
                        if s_score is not None:
                            tmp.score_ms = int(s_score)
                        det_all['static'] = s_det
                        try:
                            import json
                            (RESPONSE_DIR / f"eval_static_m{tmp.conv_id}_r{tmp.round_id}.json").write_text(
                                json.dumps({'score_ms': s_score, 'details': s_det}, ensure_ascii=False, indent=2),
                                encoding='utf-8')
                        except Exception:
                            pass
                    # 动态评估禁用：无论 eval-mode，均跳过连库实测
                    if args.eval_mode in ('dynamic', 'both') and not getattr(args, 'no_db_eval', False):
                        if not DISABLE_DYNAMIC_EVAL:
                            d_score, d_det = _eval_candidate_with_runner(
                                tmp, args, eval_db_cfg, args.schema_sql, args.eval_sql_dir, out_base,
                            )
                            if d_score is not None:
                                tmp.score_ms = int(d_score)
                            det_all['dynamic'] = d_det
                        else:
                            det_all['dynamic'] = {'note': '动态评估已禁用（未连接数据库）'}
                    if (args.eval_mode in ('static', 'both') or args.eval_mode in ('dynamic',)) and getattr(args, 'use_explain_debug', False):
                        ex_score, ex_det = _static_cost_eval_explain(
                            getattr(args, 'explain_debug_dir'), args.eval_sql_dir, tmp.ops_text,
                        )
                        if ex_score is not None:
                            tmp.score_ms = int(ex_score)
                        det_all['static_explain'] = ex_det
                    # 为优化轮的“合法序列”写入性能反馈 MD（含离线估算 + runner逐步）
                    try:
                        if getattr(args, 'use_explain_debug', False) and getattr(args, 'explain_debug_dir', None):
                            auto_pats = _infer_prune_patterns_from_ops(tmp.ops_text)
                            fb_text, _ = _build_perf_feedback(
                                tmp.ops_text,
                                explain_dir=args.explain_debug_dir,
                                storage_meta_path=getattr(args, 'storage_meta', None),
                                prune_patterns=auto_pats,
                            )
                            # 附带 runner 逐步评估表格（若存在）
                            try:
                                md_path = RESPONSE_DIR / 'auto_eval_runner.md'
                                if md_path.exists():
                                    fb2 = md_path.read_text(encoding='utf-8')
                                    fb_text = (fb_text + "\n\n" + fb2).strip() if fb_text else fb2
                            except Exception:
                                pass
                            if fb_text:
                                try:
                                    PERF_FB_DIR.mkdir(parents=True, exist_ok=True)
                                except Exception:
                                    pass
                                (PERF_FB_DIR / f"perf_fb_m{base.conv_id}_r{base.round_id + s_idx}_opt.md").write_text(fb_text, encoding='utf-8')
                    except Exception:
                        pass
                    tmp.details = det_all or {'note': '未提供评估环境，暂不计算性能得分'}
                    # accept if better
                    if isinstance(tmp.score_ms, int) and (not isinstance(best.score_ms, int) or tmp.score_ms < best.score_ms):
                        best = tmp
                    # 已获得合法序列：提前终止后续优化轮
                    print(f"[auto-loop] Finalist#{rank} 已获得合法序列，提前终止优化轮（{s_idx}/{args.opt_rounds_s}）。")
                    break
                return best

            improved: List[Candidate] = []
            for rank, cand in enumerate(finalists, start=1):
                improved.append(await _optimize_candidate(cand, rank))

            # Pick the final best among improved
            improved.sort(key=_score_key)
            current_best = improved[0]

        # Build summary CSV across all candidates we evaluated (static JSON files)
        try:
            if args.eval_sql_dir:
                _write_summary_csv(args.eval_sql_dir)
        except Exception as e:
            print(f"[auto-loop] 生成 summary.csv 失败：{e}")

        # Show and log final sequence; ask for confirmation before applying changes
        final_ops = current_best.ops_text
        REPLIES_DIR.mkdir(parents=True, exist_ok=True)
        (REPLIES_DIR / 'response_final.txt').write_text(final_ops, encoding='utf-8')
        # Derive baseline vs final evaluation
        baseline_ms = None
        final_ms = current_best.score_ms if isinstance(current_best.score_ms, int) else None
        det = current_best.details or {}
        if isinstance(det, dict):
            if 'static' in det and isinstance(det['static'], dict):
                try:
                    baseline_ms = det['static'].get('baseline_total_cost') or det['static'].get('baseline_total_ms')
                    if final_ms is None:
                        final_ms = det['static'].get('pred_total_cost') or det['static'].get('pred_total_ms')
                except Exception:
                    pass
            elif 'static_explain' in det and isinstance(det['static_explain'], dict):
                try:
                    baseline_ms = det['static_explain'].get('baseline_total_cost') or det['static_explain'].get('baseline_total_ms')
                    if final_ms is None:
                        final_ms = det['static_explain'].get('pred_total_cost') or det['static_explain'].get('pred_total_ms')
                except Exception:
                    pass
        print("\n[auto-loop] 最终选定的操作序列（暂不执行）：\n")
        print(final_ops)
        if isinstance(baseline_ms, (int, float)) or isinstance(final_ms, (int, float)):
            print("\n[auto-loop] 性能评估对比（cost）：")
            if isinstance(baseline_ms, (int, float)):
                print(f"- 初始基线：{baseline_ms:.0f} cost")
            else:
                print("- 初始基线：不可用（未进行静态基线或解析失败）")
            if isinstance(final_ms, (int, float)):
                print(f"- 最终选择：{final_ms:.0f} cost")
            else:
                print("- 最终选择：不可用（未评估）")
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        fs_text = [
            "最终操作序列：\n",
            final_ops, "\n\n",
            "性能评估对比（cost）：\n",
        ]
        if isinstance(baseline_ms, (int, float)):
            fs_text.append(f"初始基线：{baseline_ms:.0f} cost\n")
        else:
            fs_text.append("初始基线：不可用\n")
        if isinstance(final_ms, (int, float)):
            fs_text.append(f"最终选择：{final_ms:.0f} cost\n")
        else:
            fs_text.append("最终选择：不可用\n")
        (LOGS_DIR / 'final_selection.log').write_text(''.join(fs_text), encoding='utf-8')

        # 交互：选择执行模式
        mode = ''
        if args.auto_confirm:
            # 默认按现有参数执行：有 --use-db 则落库，否则 dry-run
            mode = 'db' if getattr(args, 'use_db', False) else 'dry'
        else:
            try:
                mode = input("执行方式？输入 yes/ y/ db 表示落库执行；输入 dry 仅生成改写SQL（dry-run，不连库）；直接回车取消：").strip().lower()
            except EOFError:
                mode = ''
        if mode not in ('y', 'yes', 'db', 'dry'):
            print("[auto-loop] 已取消执行。操作序列与评估对比已输出至 response/replies/response_final.txt 和 response/logs/final_selection.log。")
            return 0

        # 写入最终操作序列
        (RESPONSE_DIR / 'response.txt').write_text(final_ops, encoding='utf-8')

        # 若选择落库执行，但未显式提供 --use-db，则尝试复用 --eval-db-config 的 INI
        if mode in ('y', 'yes', 'db'):
            if not getattr(args, 'use_db', False):
                if getattr(args, 'eval_db_config', None):
                    try:
                        cfg = _read_ini_mysql(args.eval_db_config)
                        # 动态补齐 runner 所需连接参数
                        setattr(args, 'use_db', True)
                        setattr(args, 'host', cfg.get('host') or getattr(args, 'host', 'localhost'))
                        setattr(args, 'port', int(cfg.get('port') or getattr(args, 'port', 3306)))
                        setattr(args, 'user', cfg.get('user') or getattr(args, 'user', 'root'))
                        setattr(args, 'password', cfg.get('password') or getattr(args, 'password', ''))
                        setattr(args, 'database', cfg.get('database') or getattr(args, 'database', ''))
                    except Exception as e:
                        print(f"[auto-loop] 读取评估 DB 配置失败，将回退为 dry-run：{e}")
                        mode = 'dry'
                else:
                    # 未提供 DB 参数与 INI，无法落库，回退 dry-run
                    print("[auto-loop] 未提供 --use-db 或 --eval-db-config，无法连接数据库，将回退为 dry-run。")
                    mode = 'dry'
        else:
            # 强制 dry-run
            if getattr(args, 'use_db', False):
                setattr(args, 'use_db', False)

        print("[auto-loop] 正在执行选定的操作序列 … (" + ("落库" if mode in ('y','yes','db') else "dry-run") + ")")
        ok, out = _run_runner(args)
        (LOGS_DIR / 'runner_final_apply.log').write_text(out, encoding='utf-8')
        if ok:
            print("\n[auto-loop] 执行完成，详情见 response/runner_final_apply.log。")
            return 0
        print("\n[auto-loop] 执行失败，详情见 response/runner_final_apply.log。")
        return 4

    # Legacy single-conversation mode
    async with aiohttp.ClientSession() as session:
        background = ''
        prompt_text = init_text
        prev_reply: Optional[str] = None
        for i in range(1, args.max_iters + 1):
            print(f"\n[auto-loop] 第 {i} 轮：请求 LLM ...")
            # First turn: send only initial prompt as the user message
            if i == 1:
                reply = await _ask_llm(session, background=background, prompt=prompt_text)
            else:
                # On subsequent turns, include initial prompt in background; prompt contains previous reply + error logs
                background = init_text
                reply = await _ask_llm(session, background=background, prompt=prompt_text)

            # sanitize to ops-only text for runner
            ops_text = _sanitize_llm_reply_to_ops(reply)
            _write_response_n(i, ops_text)
            print(f"[auto-loop] 已保存 LLM 回复 -> response/response{i}.txt 并同步到 response.txt")

            # 2) Run runner on this response
            ok, out = _run_runner(args)
            log_path = RESPONSE_DIR / f"runner_{i}.log"
            log_path.write_text(out, encoding='utf-8')
            if ok:
                print("\n[auto-loop] 成功：runner 未检测到错误。")
                (RESPONSE_DIR / 'success.txt').write_text(
                    f"成功于第{i}轮。\n\n操作序列:\n{ops_text}\n", encoding='utf-8'
                )
                return 0

            # Not ok: build feedback for next turn
            print("\n[auto-loop] runner 检测到错误，准备反馈给 LLM 进行修正。")
            # Trim error output to a reasonable size
            max_err_chars = 4000
            err_snippet = out[-max_err_chars:]
            prev_reply = ops_text
            # Construct the repair prompt
            prompt_text = (
                "请基于上一次给出的操作序列进行修正，并继续使用相同的输出格式：\n"
                "- 只输出操作序列（不含解释、不含其他文字）\n"
                "- 使用短横线 '-' 串联多个操作，或一行一个操作\n"
                "- 使用以下支持的操作：VerticalSplit / HorizontalSplit / HorizontalMerge / TableJoin / RedundantColumnAdd / RedundantColumnDrop\n\n"
                f"上一轮操作序列：\n{prev_reply}\n\n"
                f"执行日志（截断）：\n{err_snippet}\n\n"
                "请给出新的操作序列以修复上述问题。"
            )

        # loop exhausted
        (RESPONSE_DIR / 'failure.txt').write_text(
            "未能在最大迭代次数内完成修正，请检查 runner_*.log 与 response*.txt。\n", encoding='utf-8'
        )
        print("\n[auto-loop] 结束：达到最大轮次仍未成功，已生成 failure.txt。")
        return 2


def build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser()
    ap.add_argument('--max-iters', type=int, default=5)
    ap.add_argument('--use-db', action='store_true', help='Runner 连接 MySQL 并自动补全参数')
    ap.add_argument('--host', default='localhost')
    ap.add_argument('--port', type=int, default=3306)
    ap.add_argument('--user', default='root')
    ap.add_argument('--password', default='')
    ap.add_argument('--database', default='')
    ap.add_argument('--sql-dir', help='输入 SQL 目录（可选）')
    ap.add_argument('--out-sql-dir', help='输出 SQL 目录（可选）')
    ap.add_argument('--schema-sql', help='供顺序校验使用的初始 schema.sql（可选）')
    ap.add_argument('--stats-dir', help='prepare.py 产物目录（可选，优先用于顺序校验与 runner 统一输入）')
    ap.add_argument('--storage-meta', help='存储估算元数据 JSON（启用存储预算检查）')
    ap.add_argument('--storage-budget', help='存储预算阈值，例如 10GB/500MB/100000000（字节）')
    # Pure-static options
    ap.add_argument('--baseline-json', help='预先计算的 PART2 映射 JSON（避免在评估期间连接数据库）')
    ap.add_argument('--no-db-eval', action='store_true', help='禁用所有依赖数据库的评估步骤（跳过 dynamic 与任何 DB 连接）')
    ap.add_argument('--use-explain-debug', action='store_true', help='使用 PART2_DEBUG 生成的 EXPLAIN 原始文本进行纯离线评估')
    ap.add_argument('--explain-debug-dir', default=str(ROOT / 'part2_debug'), help='PART2_DEBUG 输出目录（包含 explain/、sql/、index_map.json）')
    # Parallel selection parameters
    ap.add_argument('--parallel-m', type=int, default=0, help='并行启动的对话数量 m（0=不启用并行模式）')
    ap.add_argument('--rounds-n', type=int, default=2, help='在前 n 轮内筛选候选对话进行评估')
    ap.add_argument('--select-k', type=int, default=1, help='每轮最多评估的通过合法性检查的对话数量 k')
    ap.add_argument('--opt-rounds-s', type=int, default=0, help='最终选择后进行 s 轮优化')
    # Evaluation environment (optional)
    ap.add_argument('--eval-db-config', help='评估用 MySQL INI 配置（[mysql] 段）')
    ap.add_argument('--eval-sql-dir', help='评估用的原始 SQL 目录（将对其进行改写并测量延迟）')
    ap.add_argument('--eval-mode', choices=['static', 'dynamic', 'both'], default='static', help='评估模式：static=纯静态评估（默认采用 SQL 替换映射消除 JOIN 成本），dynamic=临时库实测，both=都做（排名优先用 dynamic）')
    # Confirmation
    ap.add_argument('--auto-confirm', action='store_true', help='自动确认最终执行（不交互）')
    # Output management
    ap.add_argument('--out-base-dir', default=str(ROOT / 'response'), help='输出基目录（默认 response）')
    ap.add_argument('--run-tag', default=None, help='将本次输出写入 out-base-dir/runs/<run-tag>；默认会在开始前清空输出目录')
    # 保留参数但不再需要显式开启；默认总是清空
    ap.add_argument('--clean-out', action='store_true', help='[兼容保留] 已默认清空，无需指定')
    return ap


# --- Summary CSV builder ---
def _write_summary_csv(sql_dir: str, out_path: Optional[Path] = None) -> None:
    """Scan response/eval_static_m*_r*.json and write response/summary.csv.
    Columns: conv_id,round_id,baseline_ms,pred_ms,improvement_ms,tables_added,tables_removed,joins_before,joins_after,joins_added,joins_removed
    Table/join diffs are computed without real SQL rewriting by applying table-mapping heuristics.
    """
    import json
    import csv
    try:
        from analysis import sql_usage_diff as sud  # type: ignore
    except Exception:
        sud = None  # type: ignore

    paths = sorted(RESPONSE_DIR.glob('eval_static_m*_r*.json'))
    if not paths:
        return
    rows: List[List[str]] = []
    header = [
        'conv_id','round_id','baseline_ms','pred_ms','improvement_ms',
        'tables_added','tables_removed','joins_before','joins_after','joins_added','joins_removed'
    ]
    for p in paths:
        m = re.search(r"eval_static_m(\d+)_r(\d+)", p.name)
        if not m:
            continue
        conv_id, round_id = int(m.group(1)), int(m.group(2))
        try:
            data = json.loads(p.read_text(encoding='utf-8'))
        except Exception:
            continue
        det = (data or {}).get('details') or {}
        base = det.get('baseline_total_ms')
        pred = det.get('pred_total_ms')
        imp = det.get('improvement_ms')

        # Table/join diff union across workload
        ops_path = REPLIES_DIR / f"response_m{conv_id}_r{round_id}.txt"
        tables_added: Set[str] = set()
        tables_removed: Set[str] = set()
        joins_before: Set[Tuple[str,str]] = set()
        joins_after: Set[Tuple[str,str]] = set()
        if sud is not None and ops_path.exists():
            ops_text = ops_path.read_text(encoding='utf-8')
            merge_map, split_map = sud._build_table_mapping(ops_text)
            for sp in Path(sql_dir).rglob('*.sql'):
                try:
                    sql = sp.read_text(encoding='utf-8', errors='ignore')
                except Exception:
                    continue
                before = sud._extract_tables(sql)
                after = sud._apply_mapping(before, merge_map, split_map)
                diff_added = after - before
                diff_removed = before - after
                tables_added.update(diff_added)
                tables_removed.update(diff_removed)
                jb = sud._extract_join_pairs(sql)
                # map joins after
                ja: Set[Tuple[str,str]] = set()
                for a,b in jb:
                    aa = merge_map.get(a, a)
                    bb = merge_map.get(b, b)
                    if a in split_map or b in split_map:
                        aa, bb = a, b
                    x,y = sorted([aa, bb])
                    ja.add((x,y))
                joins_before.update(jb)
                joins_after.update(ja)
        joins_added = joins_after - joins_before
        joins_removed = joins_before - joins_after

        rows.append([
            str(conv_id), str(round_id),
            str(int(base) if isinstance(base,(int,float)) else ''),
            str(int(pred) if isinstance(pred,(int,float)) else ''),
            str(int(imp) if isinstance(imp,(int,float)) else ''),
            ';'.join(sorted(tables_added)) or '',
            ';'.join(sorted(tables_removed)) or '',
            str(len(joins_before)), str(len(joins_after)),
            str(len(joins_added)), str(len(joins_removed)),
        ])

    out = out_path or (RESPONSE_DIR / 'summary.csv')
    with out.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    # Setup output directories; always start from a clean directory
    base = Path(args.out_base_dir)
    if args.run_tag:
        base = base / 'runs' / str(args.run_tag)
        # also prepare a convenient symlink response/latest -> this run
        try:
            latest = Path(args.out_base_dir) / 'latest'
            if latest.exists() or latest.is_symlink():
                try:
                    latest.unlink()
                except Exception:
                    pass
            latest.parent.mkdir(parents=True, exist_ok=True)
            latest.symlink_to(base, target_is_directory=True)
        except Exception:
            pass
    # 默认清空输出目录（仅清理 auto-loop 产物），不再依赖 --clean-out
    _clean_output_dir(base)
    _set_output_dirs(base)
    try:
        rc = asyncio.run(main_async(args))
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)


if __name__ == '__main__':
    main()
