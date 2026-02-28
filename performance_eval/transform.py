from __future__ import annotations
import json
import re
from typing import Dict, Any, List, Optional, Tuple, DefaultDict
from collections import defaultdict
from .plan import parse_plan, compute_total_cost, PlanNode
from .cost_models import (
    model_table_scan, model_index_scan, model_filter, model_hash_group,
    model_sort_group, model_sort, model_hash_join, model_nested_loop,
    fallback_scale, infer_join_selectivity
)


class PlanAdjuster:
    def __init__(self, plan_text: str):
        self.nodes = parse_plan(plan_text)
        # Build root list by depth=0
        self.roots = [n for n in self.nodes if n.parent is None]

    def _affects_target(self, n: PlanNode, targets: List[str]) -> bool:
        if not targets:
            return False
        ntables = set(t.strip('`') for t in n.tables)
        return any(t in ntables for t in targets)

    def _postorder(self) -> List[PlanNode]:
        order: List[PlanNode] = []
        def dfs(n: PlanNode):
            for c in n.children:
                dfs(c)
            order.append(n)
        for r in self.roots:
            dfs(r)
        return order

    def apply_type1(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Capture original total cost before any mutation
        orig_total_cost = sum((n.cost or 0.0) for n in self.nodes)
        targets = [params.get('target_table')] if params.get('target_table') else params.get('target_tables', [])
        row_factor = float(params.get('rows_factor', 1.0))
        cols_factor = float(params.get('cols_factor', 1.0))
        filter_factor = float(params.get('filter_factor', 1.0))

        # Pass 1: adjust leaves that touch target (scan/lookup)
        for n in self._postorder():
            if n.type in ('table_scan', 'index_scan', 'index_lookup') and self._affects_target(n, targets):
                if n.type == 'table_scan':
                    new_rows, new_cost = model_table_scan(n, row_factor, cols_factor)
                else:
                    new_rows, new_cost = model_index_scan(n, row_factor, filter_factor)
                n.new_rows = new_rows
                n.new_cost = new_cost

        # Pass 2: propagate up and adjust interior nodes
        for n in self._postorder():
            if n.type in ('table_scan', 'index_scan', 'index_lookup'):
                # already adjusted (if target), else keep original
                if n.new_rows is None:
                    n.new_rows = n.rows
                if n.new_cost is None:
                    n.new_cost = n.cost
                continue

            if not n.children:
                # Leaf but not a scan; keep values
                n.new_rows = n.rows
                n.new_cost = n.cost
                continue

            # Helper to get child new rows/cost
            def child(i: int) -> PlanNode:
                return n.children[i]

            if n.type == 'filter':
                cr = child(0).new_rows or child(0).rows or 0.0
                # Only scale selectivity when this filter touches target subtree
                affects = any(self._affects_target(desc, targets) for desc in n.children)
                sel_factor = filter_factor if affects else 1.0
                n.new_rows = model_filter(n, cr, sel_factor)
                # cost scaling fallback by input ratio
                input_c1 = child(0).rows or cr
                input_c2 = cr
                n.new_cost = fallback_scale(n, input_c2, input_c1, cols_factor, False)
                continue

            if n.type in ('group_temp', 'group_agg'):
                cr = child(0).new_rows or child(0).rows or 0.0
                nr, nc = model_hash_group(n, cr, cols_factor)
                n.new_rows = nr
                n.new_cost = nc
                continue

            if n.type == 'sort':
                cr = child(0).new_rows or child(0).rows or 0.0
                n.new_rows = child(0).new_rows
                n.new_cost = model_sort(n, cr, cols_factor)
                continue

            if n.type == 'hash_join':
                if len(n.children) < 2:
                    n.new_rows = n.rows
                    n.new_cost = n.cost
                    continue
                l = child(0)
                r = child(1)
                lrows = l.new_rows or l.rows or 0.0
                rrows = r.new_rows or r.rows or 0.0
                # Prefer explicit hint from synthetic joins
                join_sel = getattr(n, 'hint_join_sel', None) or infer_join_selectivity(n.rows, l.rows, r.rows)
                nr, nc = model_hash_join(n, lrows, rrows, join_sel, cols_factor)
                n.new_rows = nr
                n.new_cost = nc
                continue

            if n.type == 'nested_loop':
                if len(n.children) < 2:
                    n.new_rows = n.rows
                    n.new_cost = n.cost
                    continue
                l = child(0)
                r = child(1)
                lrows = l.new_rows or l.rows or 0.0
                rrows = r.new_rows or r.rows or 0.0
                # Output rows: keep original join selectivity
                join_sel = infer_join_selectivity(n.rows, l.rows, r.rows)
                out_rows_new = lrows * rrows * join_sel
                n.new_rows = out_rows_new
                n.new_cost = model_nested_loop(n, lrows, rrows, out_rows_new)
                continue

            # Default: scale cost by input ratio; propagate rows from child 0
            cr = child(0).new_rows or child(0).rows or 0.0
            c1 = child(0).rows or cr
            n.new_rows = n.rows if n.rows is not None else cr
            n.new_cost = fallback_scale(n, cr, c1, cols_factor, False)

        # Build text
        new_lines = [n.format_line() for n in self.nodes]
        original_total = orig_total_cost
        new_total = sum((n.new_cost if n.new_cost is not None else (n.cost or 0.0)) for n in self.nodes)
        return {
            'new_plan_text': '\n'.join(new_lines),
            'original_total_cost': original_total,
            'new_total_cost': new_total,
            'delta': new_total - original_total,
        }

    def apply_type2_remove_join(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Capture original total cost before mutation for reporting
        orig_total_cost = sum((n.cost or 0.0) for n in self.nodes)
        old_tables = set(params.get('old_tables', []))
        new_table = params.get('new_table')
        new_table_rows = float(params.get('new_table_rows', 0))

        def subtree_tables(n: PlanNode) -> set:
            s = set(n.tables)
            for c in n.children:
                s |= subtree_tables(c)
            return s

        # Replace the highest join that covers all old_tables with a scan on new_table
        candidate: Optional[PlanNode] = None
        debug_list = []
        for n in self.nodes:
            if 'join' in n.type:
                tabs = subtree_tables(n)
                try:
                    debug_list.append({'node': n.text, 'tables': sorted(list(tabs))})
                except Exception:
                    pass
                if old_tables.issubset(tabs):
                    # Prefer the shallowest (closest to root)
                    if candidate is None or n.depth < candidate.depth:
                        candidate = n

        if candidate is not None:
            # Mutate node in place to a table scan on new_table
            candidate.text = f"Table scan on {new_table}"
            candidate.type = 'table_scan'
            candidate.tables = [new_table]
            candidate.cost = new_table_rows
            candidate.rows = new_table_rows
            candidate.children = []
            candidate.new_cost = None
            candidate.new_rows = None
        else:
            # Safer offline behavior: if we cannot find a join that covers old_tables,
            # DO NOT synthesize any new scan node. Keep plan unchanged and return a no-op result.
            new_lines = [n.format_line() for n in self.nodes]
            return {
                'new_plan_text': '\n'.join(new_lines),
                'original_total_cost': orig_total_cost,
                'new_total_cost': orig_total_cost,
                'delta': 0.0,
                'note': 'no matching join subtree for old_tables; skipped',
                'debug': {'old_tables': list(old_tables), 'join_nodes': debug_list},
            }

        # After mutation, run type1 with the new table to apply width scaling if needed
        res = self.apply_type1({
            'target_table': new_table,
            'rows_factor': 1.0,
            'cols_factor': float(params.get('cols_factor', 1.0)),
            'filter_factor': 1.0,
        })
        try:
            res.setdefault('debug', {'old_tables': list(old_tables), 'join_nodes': debug_list})
        except Exception:
            pass
        return res

    def apply_type2_add_join(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Step 1: replace target with table A and apply type1
        replace_target = params.get('replace_target')
        table_a = params.get('table_a') or params.get('replace_target')
        replace_rows_factor = float(params.get('replace_rows_factor', 1.0))
        cols_factor = float(params.get('cols_factor', 1.0))
        self.apply_type1({
            'target_table': replace_target,
            'rows_factor': replace_rows_factor,
            'cols_factor': cols_factor,
            'filter_factor': float(params.get('filter_factor', 1.0)),
        })

        # Step 2: add A join B at the top (synthetic join wrapping a scan of B)
        table_b = params.get('add_table')
        rows_b = float(params.get('add_rows', 0.0))
        join_type = (params.get('join_type') or 'hash').lower()
        join_sel = float(params.get('join_sel', 1e-6))

        # Create scan for B as a new root
        scan_b = PlanNode(1, f"Table scan on {table_b}", cost=rows_b, rows=rows_b, actual_rows=None)

        # Find first node that references table_a to wrap
        anchor = None
        for n in self.nodes:
            if table_a in n.tables:
                anchor = n
                break
        if anchor is None:
            # fallback: wrap first root
            anchor = self.roots[0]

        join_label = 'Inner hash join' if join_type == 'hash' else 'Nested loop inner join'
        join_node = PlanNode(anchor.depth, join_label, cost=None, rows=None, actual_rows=None)
        join_node.hint_join_sel = join_sel
        # Rewire: place join where anchor was, with anchor and scan_b as children
        parent = anchor.parent
        join_node.children = [anchor, scan_b]
        anchor.parent = join_node
        scan_b.parent = join_node
        if parent is None:
            # replace in roots
            self.roots = [join_node if r is anchor else r for r in self.roots]
        else:
            parent.children = [join_node if c is anchor else c for c in parent.children]
            join_node.parent = parent
        # Insert into flat node list (not strictly necessary for formatting, but for total cost)
        self.nodes.append(join_node)
        self.nodes.append(scan_b)

        # Recompute costs/rows with models on the augmented tree
        # For simplicity, we run type1 with no additional scaling; join selectivity is applied inside models
        result = self.apply_type1({
            'target_table': table_b,
            'rows_factor': 1.0,
            'cols_factor': cols_factor,
            'filter_factor': 1.0,
        })
        return result

    def apply_type3_prune_filters(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Interface 3: Horizontal partitioning aware filter pruning.
        - Identify Filter nodes whose predicate matches given patterns.
        - Zero out their cost (filter eliminated by partition pruning).
        - Shrink the base table input by the filter selectivity so that the child scan/branch
          reflects scanning only the partition (child.rows -> filter.rows).

        Params:
          patterns: List[str] - substrings or regex (when regex=True) to match Filter text
          regex: bool - treat patterns as regular expressions
          combine: 'product'|'min' - how to combine multiple selectivities per table (default 'product')
          cols_factor: float - width scaling applied if later invoking type1 (default 1.0)
        """
        patterns: List[str] = params.get('patterns') or []
        use_regex = bool(params.get('regex', False))
        combine = (params.get('combine') or 'product').lower()
        cols_factor = float(params.get('cols_factor', 1.0))

        # Helper: check if a filter node matches any pattern
        def filter_matches(n: PlanNode) -> bool:
            if n.type != 'filter':
                return False
            txt = n.text
            if use_regex:
                for p in patterns:
                    try:
                        if re.search(p, txt, flags=re.IGNORECASE):
                            return True
                    except re.error:
                        continue
                return False
            else:
                return any(p.lower() in txt.lower() for p in patterns)

        # Pass 0: original total before mutation
        original_total = sum((n.cost or 0.0) for n in self.nodes)

        # Pass 1: find matching filters and compute per-table shrink factors
        factors: DefaultDict[str, float] = defaultdict(lambda: 1.0)
        matched_filters: List[PlanNode] = []
        for n in self.nodes:
            if not filter_matches(n):
                continue
            if not n.children:
                continue
            c = n.children[0]
            child_rows = c.rows if c.rows is not None else c.new_rows
            filt_rows = n.rows if n.rows is not None else n.new_rows
            if child_rows is None or filt_rows is None:
                # cannot infer selectivity; still zero out filter later
                matched_filters.append(n)
                continue
            sel = max(min(filt_rows / max(child_rows, 1e-9), 1.0), 0.0)
            # Attribute to all base tables referenced in child subtree (use immediate child's tables as approximation)
            tabs = [t.strip('`') for t in (c.tables or [])]
            if not tabs:
                # fallback: try extracting table tokens from filter text
                for t in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\.\w+", n.text):
                    tabs.append(t)
            for t in tabs:
                if combine == 'min':
                    factors[t] = min(factors[t], sel)
                else:  # product
                    factors[t] *= sel
            matched_filters.append(n)

        # Pass 2: apply per-table shrinking via type1 (rows_factor=sel), leaving filter factors = 1
        for t, f in factors.items():
            if f <= 0 or f >= 1:
                # f==1 means no change; f==0 allowed (empty partition)
                pass
            # Apply scaling on scans/lookups touching table t
            self.apply_type1({
                'target_table': t,
                'rows_factor': f,
                'cols_factor': cols_factor,
                'filter_factor': 1.0,
            })

        # Pass 3: zero out matched filters' cost and align rows to child new_rows
        for n in matched_filters:
            n.new_cost = 0.0
            if n.children:
                child_new_rows = n.children[0].new_rows if n.children[0].new_rows is not None else n.children[0].rows
                n.new_rows = child_new_rows

        # Emit result
        new_lines = [n.format_line() for n in self.nodes]
        new_total = sum((n.new_cost if n.new_cost is not None else (n.cost or 0.0)) for n in self.nodes)
        return {
            'new_plan_text': '\n'.join(new_lines),
            'original_total_cost': original_total,
            'new_total_cost': new_total,
            'delta': new_total - original_total,
        }


def type1(plan_text: str, **kwargs) -> Dict[str, Any]:
    return PlanAdjuster(plan_text).apply_type1(kwargs)


def type2_remove_join(plan_text: str, **kwargs) -> Dict[str, Any]:
    return PlanAdjuster(plan_text).apply_type2_remove_join(kwargs)


def type2_add_join(plan_text: str, **kwargs) -> Dict[str, Any]:
    return PlanAdjuster(plan_text).apply_type2_add_join(kwargs)


def type3_prune_filters(plan_text: str, **kwargs) -> Dict[str, Any]:
    return PlanAdjuster(plan_text).apply_type3_prune_filters(kwargs)
