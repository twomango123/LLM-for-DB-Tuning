from __future__ import annotations
import re
import json
from pathlib import Path
from typing import Dict, Any, List

JOIN_HEADER_RE = re.compile(r"join\s*\(cost=(?P<cost>[^)]+)\s*rows=(?P<rows>[^)]+)\)", re.IGNORECASE)
JOIN_NODE_RE = re.compile(r"->\s+(?:Nested loop .* join|.*hash join).*\(cost=(?P<cost>[^)]+)\s*rows=(?P<rows>[^)]+)\)", re.IGNORECASE)
TABLE_SCAN_RE = re.compile(r"->\s+Table scan on\s+(?P<name>[A-Za-z0-9_<>`]+).*\(cost=(?P<cost>[^)]+)\s*rows=(?P<rows>[^)]+)\)")
APPEND_RE = re.compile(r"->\s+Append\s+\(cost=(?P<cost>[^)]+)\s*rows=(?P<rows>[^)]+)\)", re.IGNORECASE)


def parse_float(s: str) -> float:
    s = s.strip()
    s = s.replace(',', '').replace('e+9', 'e9')
    # 处理区间 a..b，取上界 b
    if '..' in s:
        parts = s.split('..')
        try:
            return float(parts[-1])
        except Exception:
            pass
    try:
        return float(s)
    except Exception:
        # 兼容像 199e+9 这种写法
        s2 = s.replace('e+', 'e')
        try:
            return float(s2)
        except Exception:
            return 0.0


def median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    m = n // 2
    return (ys[m] if n % 2 else (ys[m-1] + ys[m]) / 2.0)


def extract(raw: str) -> Dict[str, Any]:
    blocks = [b.strip() for b in raw.split('\n\n') if b.strip()]
    out: Dict[str, Any] = {
        'join_samples': [],  # 每个样本：{left_rows,right_rows,join_rows,join_node_cost}
        'union_samples': []  # 每个样本：{append_cost, left_rows, right_rows}
    }
    for bl in blocks:
        # JOIN 样本：寻找第一条 join 行与两个 table scan 行
        join_m = JOIN_NODE_RE.search(bl)
        scans = TABLE_SCAN_RE.findall(bl)
        if join_m and len(scans) >= 2:
            join_cost = parse_float(join_m.group('cost'))
            join_rows = parse_float(join_m.group('rows'))
            # 两个 scan 的 rows 取前两次出现
            left_rows = parse_float(scans[0][2])
            right_rows = parse_float(scans[1][2])
            out['join_samples'].append({
                'join_rows': join_rows,
                'join_cost': join_cost,
                'left_rows': left_rows,
                'right_rows': right_rows,
            })
            continue
        # UNION 样本：寻找 Append 行与两个下游派生表的 Table scan rows
        app = APPEND_RE.search(bl)
        if app:
            append_cost = parse_float(app.group('cost'))
            # 尝试从该块中抓两个“Table scan on r/c” rows
            scans = TABLE_SCAN_RE.findall(bl)
            left_rows = right_rows = 0.0
            if len(scans) >= 2:
                left_rows = parse_float(scans[0][2])
                right_rows = parse_float(scans[1][2])
            out['union_samples'].append({
                'append_cost': append_cost,
                'left_rows': left_rows,
                'right_rows': right_rows,
                'append_rows': parse_float(app.group('rows')) if app.group('rows') else (left_rows + right_rows),
            })
    # 聚合：中位数
    def agg_join(samples: List[Dict[str, float]]):
        if not samples:
            return None
        return {
            'join_rows_med': median([s['join_rows'] for s in samples]),
            'join_cost_med': median([s['join_cost'] for s in samples]),
            'left_rows_med': median([s['left_rows'] for s in samples]),
            'right_rows_med': median([s['right_rows'] for s in samples]),
        }
    def agg_union(samples: List[Dict[str, float]]):
        if not samples:
            return None
        return {
            'append_cost_med': median([s['append_cost'] for s in samples]),
            'left_rows_med': median([s['left_rows'] for s in samples]),
            'right_rows_med': median([s['right_rows'] for s in samples]),
            'total_rows_med': median([s.get('append_rows', s['left_rows'] + s['right_rows']) for s in samples]),
        }
    return {
        'raw_count': len(blocks),
        'join': agg_join(out['join_samples']),
        'union': agg_union(out['union_samples']),
        'samples': out,
    }


def parse_union_text(text: str) -> Dict[str, Any] | None:
    if not text:
        return None
    m = APPEND_RE.search(text)
    if not m:
        return None
    return {
        'append_cost': parse_float(m.group('cost')),
        'append_rows': parse_float(m.group('rows')),
    }


def parse_union_json(text: str) -> Dict[str, Any] | None:
    if not text:
        return None
    try:
        data = json.loads(text)
    except Exception:
        return None
    try:
        qs = data['query_block']['union_result']['query_specifications']
    except Exception:
        return None
    rows = []
    for q in qs:
        qb = q.get('query_block') or {}
        tb = qb.get('table') or {}
        rpj = tb.get('rows_produced_per_join')
        if rpj is not None:
            try:
                rows.append(float(rpj))
            except Exception:
                pass
    return {
        'append_rows': sum(rows) if rows else None,
    }


def main():
    import argparse
    ap = argparse.ArgumentParser(description='Extract sample costs from raw EXPLAIN text')
    ap.add_argument('--input', default='response/samples/raw_explain_samples.txt')
    ap.add_argument('--output', default='response/samples/samples.json')
    ap.add_argument('--union_text', default=None, help='Optional: path to UNION EXPLAIN ANALYZE text')
    ap.add_argument('--union_json', default=None, help='Optional: path to UNION EXPLAIN FORMAT=JSON text')
    args = ap.parse_args()

    raw = Path(args.input).read_text(encoding='utf-8')
    res = extract(raw)

    # 覆盖/补充 UNION 样本
    ut = Path(args.union_text).read_text(encoding='utf-8') if args.union_text else None
    uj = Path(args.union_json).read_text(encoding='utf-8') if args.union_json else None
    u_text = parse_union_text(ut) if ut else None
    u_json = parse_union_json(uj) if uj else None
    if u_text or u_json:
        res.setdefault('union', res.get('union') or {})
        if u_text and (u_text.get('append_cost') is not None):
            res['union']['append_cost_med'] = float(u_text['append_cost'])
        rows = (u_text or {}).get('append_rows') or (u_json or {}).get('append_rows')
        if rows is not None:
            res['union']['total_rows_med'] = float(rows)
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(res, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f"OK: wrote {args.output}")


if __name__ == '__main__':
    main()
