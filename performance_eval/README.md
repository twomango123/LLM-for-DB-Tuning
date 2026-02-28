# Performance Evaluation Module

Provides two interfaces to project cost and plan changes from schema/behavior changes using simplified operator cost models.

- Interface 1: Re-estimate costs when a target table's rows/columns/selectivity change. Outputs adjusted EXPLAIN-like tree and total cost delta.
- Interface 2: Apply behavior changes to joins (remove/replace or add a new join), chaining Interface 1 where required, and return final adjusted plan and total cost.
- Interface 2.5 (bridge): Call rewrite-ops' evaluate_on_plan() to enumerate plan/cost changes per operation. See performance_eval/eval_bridge.py.

This module parses MySQL EXPLAIN ANALYZE trees in the repo format ("->" indented). It updates node `cost=` and `rows=` when possible; nodes without `cost=` are left unchanged except `rows=` when derivable.

Usage:

```bash
python -m performance_eval.cli type1 \
  --plan part2_debug/explain/q2.txt \
  --target t2 \
  --rows-factor 1.5 \
  --cols-factor 1.2 \
  --filter-factor 0.8 \
  --output out.json

python -m performance_eval.cli type2 remove-join \
  --plan part2_debug/explain/q21.txt \
  --old-tables a b \
  --new-table ab \
  --new-table-rows 500000 \
  --output out.json

python -m performance_eval.cli type2 add-join \
  --plan part2_debug/explain/q2.txt \
  --replace-target t2 --replace-rows-factor 0.7 \
  --add-table B --add-rows 100000 --join-type hash --join-sel 0.001 \
  --post-rows-factor 1.2 \
  --output out.json
```

Notes:
- Total cost is the sum of per-node costs present in the plan (after adjustment). For nodes missing `cost=`, we may infer cost from models, else treat as 0.
- For Interface 1, we propagate row changes up the tree: Filter, Join, Group, Sort. When formulas are insufficient, fallback scaling rule `new_cost = old_cost * (C2/C1)` is used where `C1` is old input cardinality and `C2` is new input cardinality.
- Column change is modeled as width factor. It scales costs of operators sensitive to row width: scans, sorts, hash/group, materialization.

Limitations:
- This is a heuristic estimator, not a full optimizer. It assumes stable join selectivity unless specified; infers it from original `rows=` when possible.
- Parsing depends on the repo's EXPLAIN formatting; other formats may require tweaks.
- Evaluators in rewrite/ are heuristics to match requested rules (keep_old vs not; vertical/horizontal splits, merges, and redundant columns). They mainly scale costs via type1/2 transforms and optionally add a sampled UNION cost.
