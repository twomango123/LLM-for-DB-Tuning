from __future__ import annotations
import argparse
import importlib
import json
from pathlib import Path
from typing import List

from .eval_bridge import run_eval_sequence


def load_ops_from_runner_log(path: Path) -> List[object]:
    """
    Best-effort loader that imports rewrite classes and constructs op instances
    from the runner log context is not trivial; here we expect a Python file to
    import and construct the operations list if needed. This keeps CLI generic.
    For quick usage we allow passing a module path like rewrite_ops:ops_list.
    """
    raise SystemExit("Please provide --ops-module rewrite_ops:ops list, or integrate with your pipeline.")


def main():
    ap = argparse.ArgumentParser(description='Bridge: evaluate_on_plan over rewrite ops')
    ap.add_argument('--plan', required=True)
    ap.add_argument('--ops-module', required=True, help='Module path like pkg.mod:var holding list[op]')
    ap.add_argument('--meta', default='output_dir/meta.json')
    ap.add_argument('--union-sample-cost', type=float, default=None)
    ap.add_argument('--union-sample-rows', type=float, default=None)
    ap.add_argument('--out', default='-')
    args = ap.parse_args()

    plan_text = Path(args.plan).read_text(encoding='utf-8')

    mod_name, var_name = args.ops_module.split(':', 1)
    mod = importlib.import_module(mod_name)
    ops = getattr(mod, var_name)
    res = run_eval_sequence(plan_text, ops, meta_path=args.meta, extras={'union': {'sample_cost': args.union_sample_cost, 'sample_rows': args.union_sample_rows}})

    out = json.dumps(res, ensure_ascii=False, indent=2)
    if args.out == '-':
        print(out)
    else:
        Path(args.out).write_text(out, encoding='utf-8')


if __name__ == '__main__':
    main()

