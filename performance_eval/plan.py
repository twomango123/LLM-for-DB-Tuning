import re
from typing import List, Optional, Dict, Any

NUM_RE = r"[0-9]+(?:\.[0-9]+)?(?:e[+-]?[0-9]+)?"
COST_ROWS_RE = re.compile(r"\(cost=(?P<cost>" + NUM_RE + r")(?: rows=(?P<rows>" + NUM_RE + r"))?\)")
ACTUAL_ROWS_RE = re.compile(r"\(actual [^)]*rows=(?P<rows>" + NUM_RE + r")[^)]*\)")
# Capture actual time range if present: actual time=lo..hi
ACTUAL_TIME_RE = re.compile(r"\(actual[^)]*time=(?P<lo>" + NUM_RE + r")\.\.(?P<hi>" + NUM_RE + r")[^)]*\)")
ROWS_INLINE_RE = re.compile(r"rows=(?P<rows>" + NUM_RE + r")")

OP_PATTERNS = {
    'table_scan': re.compile(r"Table scan on (?P<table>[A-Za-z0-9_<>`]+)"),
    'index_scan': re.compile(r"(Covering )?Index scan on (?P<table>[A-Za-z0-9_<>`]+)"),
    'index_lookup': re.compile(r"Index lookup on (?P<table>[A-Za-z0-9_<>`]+)"),
    'single_row_lookup': re.compile(r"Single-row index lookup on (?P<table>[A-Za-z0-9_<>`]+)"),
    'filter': re.compile(r"^Filter:"),
    'sort': re.compile(r"^Sort:"),
    'limit': re.compile(r"^Limit:"),
    'hash_join': re.compile(r"hash join", re.IGNORECASE),
    'nested_loop': re.compile(r"Nested loop (inner|anti|left|semi) join|Nested loop inner join|Nested loop antijoin", re.IGNORECASE),
    'hash_build': re.compile(r"^Hash$"),
    'group_temp': re.compile(r"Aggregate using temporary table|Temporary table with deduplication", re.IGNORECASE),
    'group_agg': re.compile(r"Group aggregate|Aggregate:|HashGroup", re.IGNORECASE),
    'stream': re.compile(r"^Stream results"),
    'materialize': re.compile(r"Materialize( with deduplication)?", re.IGNORECASE),
}


def parse_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(s)
    except Exception:
        return None


class PlanNode:
    def __init__(self, depth: int, text: str, cost: Optional[float], rows: Optional[float], actual_rows: Optional[float]):
        self.depth = depth
        self.text = text.strip()
        self.cost = cost
        self.rows = rows
        self.actual_rows = actual_rows
        self.children: List['PlanNode'] = []
        self.parent: Optional['PlanNode'] = None
        self.type = self._infer_type()
        self.tables = self._infer_tables()
        # Computed/adjusted values
        self.new_rows: Optional[float] = None
        self.new_cost: Optional[float] = None
        # Optional hint for join selectivity (used in type2 add-join)
        self.hint_join_sel: Optional[float] = None
        # Observed actual time upper bound (seconds) when present
        self.actual_time: Optional[float] = None

    def _infer_type(self) -> str:
        t = self.text
        for k, pat in OP_PATTERNS.items():
            if pat.search(t):
                return k
        # default
        if t.startswith('Table scan'):
            return 'table_scan'
        if t.startswith('Index scan') or t.startswith('Covering index scan'):
            return 'index_scan'
        if t.startswith('Index lookup') or t.startswith('Single-row index lookup'):
            return 'index_lookup'
        if t.startswith('Filter:'):
            return 'filter'
        if t.startswith('Sort:'):
            return 'sort'
        if t.startswith('Limit:'):
            return 'limit'
        return 'other'

    def _infer_tables(self) -> List[str]:
        t = self.text
        for key in ['table_scan', 'index_scan', 'index_lookup', 'single_row_lookup']:
            m = OP_PATTERNS[key].search(t) if key in OP_PATTERNS else None
            if m:
                table = m.group('table')
                return [table.strip('`')]
        # No direct table; maybe in predicates like "using PRIMARY (col=tbl.col)"
        tables = set()
        for name in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\.", t):
            tables.add(name)
        # Fallback for "Nested loop inner join" lines that may not include explicit table tokens
        # Try to capture alias hints like "on t2 using PRIMARY" or filter lines mentioning t3.
        if not tables and ('join' in self.type):
            for name in re.findall(r"\b(t\d+)\b", t):
                tables.add(name)
        return list(tables)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'depth': self.depth,
            'text': self.text,
            'type': self.type,
            'tables': self.tables,
            'cost': self.cost,
            'rows': self.rows,
            'actual_rows': self.actual_rows,
            'new_cost': self.new_cost,
            'new_rows': self.new_rows,
            'children': [c.to_dict() for c in self.children],
        }

    def format_line(self) -> str:
        # Reconstruct line with updated cost/rows if present
        indent = '    ' * self.depth
        line = indent + '-> ' + self.text
        # Determine if we had a cost parenthesis originally
        # Replace cost and rows if any
        def repl_cost_rows(m: re.Match) -> str:
            old = m.group(0)
            cost = self.new_cost if self.new_cost is not None else self.cost
            rows = self.new_rows if self.new_rows is not None else self.rows
            if cost is None and rows is None:
                return old
            if cost is not None and rows is not None:
                return f"(cost={cost:.6g} rows={rows:.6g})"
            if cost is not None:
                return f"(cost={cost:.6g})"
            if rows is not None:
                return f"(rows={rows:.6g})"
            return old
        if COST_ROWS_RE.search(line):
            line = COST_ROWS_RE.sub(repl_cost_rows, line)
        else:
            # If no cost parenthesis originally, append one if we computed something
            cost = self.new_cost if self.new_cost is not None else self.cost
            rows = self.new_rows if self.new_rows is not None else self.rows
            if cost is not None or rows is not None:
                parts = []
                if cost is not None:
                    parts.append(f"cost={cost:.6g}")
                if rows is not None:
                    parts.append(f"rows={rows:.6g}")
                line += "  (" + ' '.join(parts) + ")"
        # Keep actual rows/time as-is (optional enhancement: update rows inline)
        return line


def parse_plan(text: str) -> List[PlanNode]:
    lines = [ln.rstrip('\n') for ln in text.splitlines() if ln.strip()]
    nodes: List[PlanNode] = []
    stack: List[PlanNode] = []
    for ln in lines:
        if '->' not in ln:
            continue
        depth = (len(ln) - len(ln.lstrip(' '))) // 4
        after_arrow = ln.split('->', 1)[1].strip()
        # Extract cost/rows from the portion
        cost = None
        rows = None
        m = COST_ROWS_RE.search(after_arrow)
        if m:
            cost = parse_float(m.group('cost'))
            rows = parse_float(m.group('rows'))
        else:
            # Try to find rows in actual section
            m2 = ACTUAL_ROWS_RE.search(after_arrow)
            if m2:
                rows = parse_float(m2.group('rows'))
        node = PlanNode(depth, after_arrow.split('  (actual')[0].rstrip(), cost, rows, None)
        # Extract actual time upper bound if present
        m3 = ACTUAL_TIME_RE.search(after_arrow)
        if m3:
            try:
                node.actual_time = float(m3.group('hi'))
            except Exception:
                node.actual_time = None
        # Attach to tree by depth
        while stack and stack[-1].depth >= depth:
            stack.pop()
        if stack:
            stack[-1].children.append(node)
            node.parent = stack[-1]
        nodes.append(node)
        stack.append(node)
    return nodes


def compute_total_cost(nodes: List[PlanNode]) -> float:
    total = 0.0
    for n in nodes:
        c = n.new_cost if n.new_cost is not None else n.cost
        if c is not None:
            total += c
            continue
        # Fallbacks: some formats omit cost (e.g., "Count rows in ...").
        # Prefer "actual time" upper bound if present, else approximate by rows.
        if getattr(n, 'actual_time', None) is not None:
            try:
                total += float(n.actual_time)
                continue
            except Exception:
                pass
        if n.rows is not None:
            try:
                total += float(n.rows)
            except Exception:
                pass
    return total
