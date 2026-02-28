from __future__ import annotations
"""
Eval bridge: enumerate rewrite operations and call their evaluate_on_plan()
methods when available, aggregating EXPLAIN plan text and total cost deltas.

Used to support: "在某个操作预选时调用函数评估性能变化趋势".
"""
from typing import Dict, Any, List, Optional

from .plan import compute_total_cost, parse_plan


def run_eval_sequence(plan_text: str,
                      ops: List[Any],
                      meta_path: Optional[str] = 'output_dir/meta.json',
                      extras: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Run evaluate_on_plan sequentially if an op has it; otherwise, keep plan.

    extras can include sampled operator costs such as:
      { 'union': { 'sample_cost': 1000.0, 'sample_rows': 1e6 } }
    which will be passed to evaluators that accept these kwargs.
    """
    extras = extras or {}
    cur_plan = plan_text
    # Maintain a sequentially rewritten SQL string (temporary) to决定每步是否“命中”
    cur_sql: Optional[str] = None
    try:
        if extras and isinstance(extras.get('sql_text'), str):
            cur_sql = str(extras.get('sql_text'))
    except Exception:
        cur_sql = None
    steps: List[Dict[str, Any]] = []
    total_before = compute_total_cost(parse_plan(cur_plan))

    # --- cumulative storage meta: apply each op to meta for next step ---
    cur_meta_path = meta_path
    model = None
    try:
        if meta_path:
            import json as _json
            from pathlib import Path as _Path
            from scripts.storage_transformer import StorageModel  # type: ignore
            _meta_obj = _json.loads(_Path(meta_path).read_text(encoding='utf-8'))
            model = StorageModel(_meta_obj)
            # Prepare a temp meta path under response/
            tmp_dir = _Path('response')
            tmp_dir.mkdir(parents=True, exist_ok=True)
            cur_meta_path = str(tmp_dir / 'tmp_eval_meta.json')
            _Path(cur_meta_path).write_text(_json.dumps(model.meta, ensure_ascii=False), encoding='utf-8')
    except Exception:
        model = None

    def _apply_storage_op(op_inst: Any) -> None:
        if model is None:
            return
        try:
            # Map known rewrite instances to storage operation strings
            s = None
            cname = op_inst.__class__.__name__
            if cname == 'TableJoin':
                # Expect attributes: old_tables, new_table, join_key (list of pairs), sign (1->False, 2->True)
                t1, t2 = op_inst.old_tables[0], op_inst.old_tables[1]
                keep_old = 'True' if getattr(op_inst, 'sign', 1) != 1 else 'False'
                jk = getattr(op_inst, 'join_key', None) or []
                if isinstance(jk, (list, tuple)) and len(jk) >= 1:
                    k1, k2 = jk[0][0], jk[0][1]
                else:
                    # Fallback: if no explicit keys, skip meta apply
                    return
                newt = op_inst.new_table
                s = f"TableJoin({t1}, {t2}, {t1}.{k1}, {t2}.{k2}, {keep_old}):{newt}"
            elif cname == 'TableSplit' or cname == 'VerticalSplit':
                src = getattr(op_inst, 'old_table', None) or getattr(op_inst, 'table', None) or getattr(op_inst, 'src_table', None)
                is_retained = 'True' if getattr(op_inst, 'is_retained', False) else 'False'
                new_tables = list(getattr(op_inst, 'new_tables', []) or [])
                colmap = getattr(op_inst, 'columnList', {}) or {}
                if not src or not new_tables or not colmap:
                    return
                body = []
                for nt in new_tables:
                    cols = colmap.get(nt, []) or []
                    body.append(f"{nt}({', '.join(cols)})")
                s = f"VerticalSplit({src}, {is_retained}):" + ",".join(body)
            elif cname == 'RedundantColumnAdd':
                st = op_inst.source_table; sc = op_inst.source_column
                tt = op_inst.target_table; tc = op_inst.new_column
                s = f"RedundantColumnAdd({st}.{sc}, {tt}.{tc})"
            elif cname == 'RedundantColumnDrop':
                tt = op_inst.target_table if hasattr(op_inst, 'target_table') else getattr(op_inst, 'table', None)
                rc = op_inst.redundant_column if hasattr(op_inst, 'redundant_column') else getattr(op_inst, 'column', None)
                if not tt or not rc:
                    return
                s = f"RedundantColumnDrop({tt}.{rc})"
            elif cname == 'HorizontalSplit':
                src = op_inst.table
                is_retained = 'True' if getattr(op_inst, 'is_retained', False) else 'False'
                parts = []
                for (nt, pred) in getattr(op_inst, 'predicates', []) or []:
                    parts.append(f"{nt}({pred})")
                s = f"HorizontalSplit({src}, {is_retained}):" + ",".join(parts)
            elif cname == 'HorizontalMerge':
                t1, t2 = op_inst.sources[0], op_inst.sources[1]
                is_retained = 'True' if getattr(op_inst, 'is_retained', False) else 'False'
                newt = op_inst.new_table
                s = f"HorizontalMerge({t1}, {t2}, {is_retained}):{newt}"
            if s:
                model.apply(s)
                # persist to temp path for next step evaluators
                from pathlib import Path as _Path
                import json as _json
                _Path(cur_meta_path).write_text(_json.dumps(model.meta, ensure_ascii=False), encoding='utf-8')
        except Exception:
            pass

    # Helpers for per-step meta snapshot
    def _related_tables(op_inst: Any) -> set:
        out = set()
        cname = op_inst.__class__.__name__
        try:
            if cname == 'TableJoin':
                out.update([op_inst.old_tables[0], op_inst.old_tables[1], op_inst.new_table])
            elif cname in ('TableSplit', 'VerticalSplit'):
                src = getattr(op_inst, 'old_table', None) or getattr(op_inst, 'table', None) or getattr(op_inst, 'src_table', None)
                if src:
                    out.add(src)
                for nt in getattr(op_inst, 'new_tables', []) or []:
                    out.add(nt)
            elif cname == 'HorizontalSplit':
                out.add(getattr(op_inst, 'table', ''))
                for (nt, _pred) in getattr(op_inst, 'predicates', []) or []:
                    out.add(nt)
            elif cname == 'HorizontalMerge':
                for s in getattr(op_inst, 'sources', []) or []:
                    out.add(s)
                out.add(getattr(op_inst, 'new_table', ''))
            elif cname == 'RedundantColumnAdd':
                out.add(getattr(op_inst, 'target_table', ''))
            elif cname == 'RedundantColumnDrop':
                out.add(getattr(op_inst, 'target_table', None) or getattr(op_inst, 'table', ''))
        except Exception:
            pass
        return set(x for x in out if x)

    def _row_bytes_of(meta: Dict[str, Any], t: str) -> float:
        try:
            cols = (meta.get('tables', {}).get(t, {}) or {}).get('columns', {})
            s = 0.0
            for cv in cols.values():
                avg = float((cv or {}).get('avg_length', 0.0) or 0.0)
                nf = float((cv or {}).get('null_frac', 0.0) or 0.0)
                s += avg * (1.0 - nf)
            return s
        except Exception:
            return 0.0

    for op in ops:
        if not hasattr(op, 'evaluate_on_plan'):
            steps.append({'op': type(op).__name__, 'note': 'no evaluator', 'plan_cost': None})
            # 仍需推进存储模型（有些操作虽无评估器，但对存储有影响）
            _apply_storage_op(op)
            continue
        # Best-effort: forward compatible signature
        kwargs = {'meta_path': cur_meta_path}
        # union sampling for HorizontalSplit/HorizontalMerge
        if isinstance(getattr(op, '__class__'), type):
            cname = op.__class__.__name__
            if cname in ('HorizontalSplit', 'HorizontalMerge'):
                u = (extras.get('union') or {})
                kwargs.update({
                    'sample_union_cost': u.get('sample_cost'),
                    'sample_union_rows': u.get('sample_rows'),
                })
            if cname == 'TableJoin':
                # 传入原始 SQL 用于别名映射（若提供）
                if cur_sql is not None:
                    kwargs['sql_text'] = cur_sql
        # Try to apply SQL rewrite to build sequential "temporary SQL"
        sql_changed = None
        if cur_sql is not None and hasattr(op, 'apply_to_sql'):
            try:
                new_sql = op.apply_to_sql(cur_sql)
                # 简单归一化比较：压缩空白
                def _norm(s: str) -> str:
                    return ' '.join((s or '').split())
                sql_changed = (_norm(new_sql) != _norm(cur_sql))
                cur_sql = new_sql
            except Exception:
                sql_changed = None
        # before cost for cumulative reporting
        before_this = compute_total_cost(parse_plan(cur_plan))
        res = op.evaluate_on_plan(cur_plan, **kwargs)  # type: ignore
        cur_plan = res.get('new_plan_text', cur_plan)
        after_this = compute_total_cost(parse_plan(cur_plan))
        delta_step = None
        if before_this is not None and after_this is not None:
            try:
                delta_step = float(after_this) - float(before_this)
            except Exception:
                delta_step = None
        # Per-step meta snapshot of related tables
        meta_after = None
        if model is not None:
            try:
                rel = _related_tables(op)
                snap: Dict[str, Dict[str, float]] = {}
                for t in rel:
                    mt = model.meta
                    rows = float(((mt.get('tables', {}) or {}).get(t, {}) or {}).get('row_count', 0) or 0)
                    rbytes = _row_bytes_of(mt, t)
                    snap[t] = {'row_count': rows, 'row_bytes': rbytes}
                meta_after = snap or None
            except Exception:
                meta_after = None

        steps.append({
            'op': type(op).__name__,
            'result': res,
            'after_total_cost': after_this,
            'cumulative_delta': (after_this - total_before) if (after_this is not None and total_before is not None) else None,
            'delta_step': delta_step,
            'sql_changed': sql_changed,
            'meta_after': meta_after,
        })
        # 累积更新存储模型，再将最新 meta 提供给下一步评估
        _apply_storage_op(op)

    total_after = compute_total_cost(parse_plan(cur_plan))
    return {
        'final_plan_text': cur_plan,
        'before_total_cost': total_before,
        'after_total_cost': total_after,
        'delta': (total_after - total_before) if (total_after is not None and total_before is not None) else None,
        'steps': steps,
    }
