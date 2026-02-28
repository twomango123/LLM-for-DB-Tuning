import json
import re
from copy import deepcopy
from typing import Dict, List, Tuple, Any


class StorageModel:
    def __init__(self, meta: Dict[str, Any]):
        self.meta = deepcopy(meta)
        # Normalize optional sections
        self.meta.setdefault("tables", {})
        self.meta.setdefault("foreign_keys", [])
        self.meta.setdefault("stats", {})
        self.meta["stats"].setdefault("predicates", {})
        self.meta["stats"].setdefault("joins", {})

    # -------------------------------
    # Basic model helpers
    # -------------------------------
    def table(self, name: str) -> Dict[str, Any]:
        if name not in self.meta["tables"]:
            raise KeyError(f"Table not found: {name}")
        return self.meta["tables"][name]

    def ensure_table(self, name: str, row_count: int = 0) -> Dict[str, Any]:
        t = self.meta["tables"].setdefault(name, {
            "row_count": row_count,
            "primary_key": [],
            "columns": {},
        })
        t.setdefault("row_count", row_count)
        t.setdefault("primary_key", [])
        t.setdefault("columns", {})
        return t

    def col_size(self, table: str, column: str) -> float:
        t = self.table(table)
        c = t["columns"].get(column)
        if c is None:
            raise KeyError(f"Column not found: {table}.{column}")
        avg = float(c.get("avg_length", 0))
        null_frac = float(c.get("null_frac", 0))
        return avg * (1.0 - null_frac)

    def table_row_count(self, name: str) -> int:
        return int(self.table(name).get("row_count", 0))

    def compute_total_storage(self) -> int:
        total = 0.0
        for tname, t in self.meta["tables"].items():
            rc = int(t.get("row_count", 0))
            per_row = 0.0
            for cname, c in t.get("columns", {}).items():
                avg = float(c.get("avg_length", 0))
                null_frac = float(c.get("null_frac", 0))
                per_row += avg * (1.0 - null_frac)
            total += per_row * rc
        return int(total)

    def update_total_storage(self) -> None:
        self.meta["total_storage_bytes"] = self.compute_total_storage()

    # -------------------------------
    # Operation parsing
    # -------------------------------
    _OP_RE = re.compile(r"^(?P<name>[A-Za-z]+)\((?P<args>[^)]*)\)(?::(?P<body>.*))?$")

    @staticmethod
    def _split_csv(s: str) -> List[str]:
        # Split on commas, ignoring commas inside parentheses
        out, buf, depth = [], [], 0
        for ch in s:
            if ch == '(':
                depth += 1
                buf.append(ch)
            elif ch == ')':
                depth = max(0, depth - 1)
                buf.append(ch)
            elif ch == ',' and depth == 0:
                token = ''.join(buf).strip()
                if token:
                    out.append(token)
                buf = []
            else:
                buf.append(ch)
        token = ''.join(buf).strip()
        if token:
            out.append(token)
        return out

    @classmethod
    def parse_operation(cls, op: str) -> Tuple[str, List[str], List[str]]:
        op = op.strip()
        m = cls._OP_RE.match(op)
        if not m:
            raise ValueError(f"Invalid operation format: {op}")
        name = m.group('name')
        args_str = m.group('args').strip()
        body_str = (m.group('body') or '').strip()
        args = [a.strip() for a in cls._split_csv(args_str)] if args_str else []
        body = [b.strip() for b in cls._split_csv(body_str)] if body_str else []
        return name, args, body

    # -------------------------------
    # Operation handlers
    # -------------------------------
    def apply(self, operation: str) -> Dict[str, Any]:
        name, args, body = self.parse_operation(operation)
        handler = getattr(self, f"_op_{name}", None)
        if not handler:
            raise NotImplementedError(f"Unsupported operation: {name}")
        handler(args, body)
        self.update_total_storage()
        return self.meta

    # ColumnRename(SourceTable.OldColumnName, NewColumnName)
    def _op_ColumnRename(self, args: List[str], body: List[str]) -> None:
        if len(args) != 2:
            raise ValueError("ColumnRename requires 2 args: SourceTable.OldColumnName, NewColumnName")
        src = args[0]
        new_name = args[1]
        if '.' not in src:
            raise ValueError("First arg must be Table.Column")
        table, old_col = src.split('.', 1)
        t = self.table(table)
        cols = t["columns"]
        if old_col not in cols:
            raise KeyError(f"Column not found: {table}.{old_col}")
        if new_name in cols:
            raise ValueError(f"Target column already exists: {table}.{new_name}")
        cols[new_name] = cols.pop(old_col)

    # ColumnSplit(SourceTable.Column, is_retained):NewCol1(...),NewCol2(...)
    def _op_ColumnSplit(self, args: List[str], body: List[str]) -> None:
        if len(args) != 2:
            raise ValueError("ColumnSplit requires 2 args: SourceTable.Column, is_retained")
        src, is_retained = args
        is_retained = self._parse_bool(is_retained)
        if '.' not in src:
            raise ValueError("First arg must be Table.Column")
        table, col = src.split('.', 1)
        t = self.table(table)
        cols = t["columns"]
        if col not in cols:
            raise KeyError(f"Column not found: {table}.{col}")
        # New column names are in body, each token like: NewCol(expr)
        new_cols = []
        for item in body:
            m = re.match(r"^(?P<name>[A-Za-z0-9_]+)\(.*\)$", item)
            if not m:
                raise ValueError(f"Invalid new column spec: {item}")
            new_cols.append(m.group('name'))
        if not new_cols:
            raise ValueError("ColumnSplit needs at least one new column in body")
        # Assumption: split average size equally among new columns (unless hints provided)
        orig_avg = float(cols[col].get("avg_length", 0))
        orig_null = float(cols[col].get("null_frac", 0))
        hint_key = f"split_size_hints:{table}.{col}"
        size_hints = self.meta.get("stats", {}).get(hint_key)
        if size_hints and isinstance(size_hints, dict):
            # Expect mapping new_col -> avg_length
            for nc in new_cols:
                cols[nc] = {"avg_length": float(size_hints.get(nc, 0)), "null_frac": orig_null}
        else:
            part = orig_avg / len(new_cols) if len(new_cols) > 0 else 0
            for nc in new_cols:
                cols[nc] = {"avg_length": part, "null_frac": orig_null}
        if not is_retained:
            cols.pop(col)

    # VerticalSplit(SourceTable, is_retained):T1(cols...),T2(cols...)[, T1(pk...), T2(pk...)]
    def _op_VerticalSplit(self, args: List[str], body: List[str]) -> None:
        if len(args) != 2:
            raise ValueError("VerticalSplit requires 2 args: SourceTable, is_retained")
        src_table = args[0]
        is_retained = self._parse_bool(args[1])
        t = self.table(src_table)
        src_pk = set(t.get("primary_key", []))
        src_cols = t["columns"]

        # Parse child table column specs and optional pk specs
        child_cols: Dict[str, List[str]] = {}
        child_pk: Dict[str, List[str]] = {}
        for item in body:
            m = re.match(r"^(?P<t>[A-Za-z0-9_]+)\((?P<cols>[^)]*)\)$", item)
            if not m:
                raise ValueError(f"Invalid child spec: {item}")
            cname = m.group('t')
            attrs = [a.strip() for a in self._split_csv(m.group('cols')) if a.strip()]
            # Heuristic: If all attrs are in src_pk or subset, and we already saw a child
            # with same name in child_cols, treat as pk spec.
            if cname in child_cols and set(attrs).issubset(src_pk):
                child_pk[cname] = attrs
            else:
                child_cols[cname] = attrs

        if not child_cols:
            raise ValueError("VerticalSplit needs at least one child table spec in body")

        # Create children
        for cname, attrs in child_cols.items():
            # Default child rows = parent's row_count
            default_rows = int(t.get("row_count", 0))
            # If caller provided PK spec for this child, prefer it
            pk_spec = child_pk.get(cname)
            # Allow stats hint to refine child row count by distinct on PK spec
            # stats key format: distinct:SourceTable:col1,col2
            hinted_rows = None
            if pk_spec:
                key = f"distinct:{src_table}:{','.join(pk_spec)}"
                hinted_rows = self.meta.get("stats", {}).get(key)
            ct = self.ensure_table(cname, row_count=int(hinted_rows) if isinstance(hinted_rows, int) else default_rows)
            ct_cols = {}
            # Ensure PK columns present
            attrs_set = set(attrs)
            if not src_pk.issubset(attrs_set):
                attrs = list(attrs_set.union(src_pk))
            for a in attrs:
                if a not in src_cols:
                    raise KeyError(f"Column {a} not in source table {src_table}")
                ct_cols[a] = deepcopy(src_cols[a])
            ct["columns"] = ct_cols
            # Use child pk spec if provided; otherwise inherit source pk
            if pk_spec:
                ct["primary_key"] = list(pk_spec)
            else:
                ct["primary_key"] = list(src_pk)

        if not is_retained:
            # Drop original table
            self.meta["tables"].pop(src_table, None)

    # TableJoin(Table1,Table2, key1, key2, is_retained): NewTable
    def _op_TableJoin(self, args: List[str], body: List[str]) -> None:
        if len(args) != 5 or len(body) != 1:
            raise ValueError("TableJoin requires: (T1,T2,key1,key2,is_retained): NewTable")
        t1, t2, k1, k2, is_retained = args
        is_retained = self._parse_bool(is_retained)
        new_table = body[0]
        T1, T2 = self.table(t1), self.table(t2)
        # Determine join row count
        join_key_sig = f"{t1}.{k1}={t2}.{k2}"
        stats_rows = self.meta["stats"]["joins"].get(join_key_sig)
        if isinstance(stats_rows, int):
            out_rows = int(stats_rows)
        else:
            out_rows = self._infer_join_rows(t1, t2, k1, k2)

        # Columns: union, but de-duplicate one copy of join key columns
        new_cols: Dict[str, Dict[str, Any]] = {}
        for cn, c in T1["columns"].items():
            new_cols[cn] = deepcopy(c)
        for cn, c in T2["columns"].items():
            if cn == k2 and k1 in new_cols:
                # Skip duplicate join key from T2
                continue
            if cn in new_cols:
                # If duplicate non-key name exists, prefix with table name to avoid overwrite
                new_cols[f"{t2}_{cn}"] = deepcopy(c)
            else:
                new_cols[cn] = deepcopy(c)
        # Create new table
        nt = self.ensure_table(new_table, row_count=out_rows)
        nt["columns"] = new_cols
        # Primary key: if T1.k1 is pk and T2.k2 is fk, use T1 pk. Otherwise empty.
        if k1 in T1.get("primary_key", []):
            nt["primary_key"] = [k1]
        else:
            nt["primary_key"] = []

        if not is_retained:
            # Drop originals
            self.meta["tables"].pop(t1, None)
            self.meta["tables"].pop(t2, None)

    # HorizontalSplit(SourceTable, is_retained):T1(pred),T2(pred),...
    def _op_HorizontalSplit(self, args: List[str], body: List[str]) -> None:
        if len(args) != 2:
            raise ValueError("HorizontalSplit requires 2 args: SourceTable, is_retained")
        src_table = args[0]
        is_retained = self._parse_bool(args[1])
        t = self.table(src_table)
        total_rows = int(t.get("row_count", 0))

        # Parse child specs: Name(predicate)
        children: List[Tuple[str, str]] = []
        for item in body:
            m = re.match(r"^(?P<n>[A-Za-z0-9_]+)\((?P<p>.*)\)$", item)
            if not m:
                raise ValueError(f"Invalid child spec: {item}")
            children.append((m.group('n'), m.group('p').strip()))
        if not children:
            raise ValueError("HorizontalSplit needs at least one child table spec in body")

        # Assign rows based on stats.predicates or even split fallback
        assigned_total = 0
        remaining_children = []
        for cname, pred in children:
            key = f"{src_table}:{pred}"
            rows = self.meta["stats"]["predicates"].get(key)
            if isinstance(rows, int):
                cr = int(rows)
                assigned_total += cr
                ct = self.ensure_table(cname, row_count=cr)
                ct["columns"] = deepcopy(t["columns"])  # same schema
                ct["primary_key"] = deepcopy(t.get("primary_key", []))
            else:
                remaining_children.append((cname, pred))

        # Evenly distribute remaining rows
        remaining_rows = max(0, total_rows - assigned_total)
        default_each = remaining_rows // max(1, len(remaining_children)) if remaining_children else 0
        for i, (cname, pred) in enumerate(remaining_children):
            cr = default_each + (1 if i == 0 and remaining_rows % max(1, len(remaining_children)) != 0 else 0)
            ct = self.ensure_table(cname, row_count=cr)
            ct["columns"] = deepcopy(t["columns"])  # same schema
            ct["primary_key"] = deepcopy(t.get("primary_key", []))

        if not is_retained:
            self.meta["tables"].pop(src_table, None)

    # HorizontalMerge(Table1, Table2, is_retained):NewTable
    def _op_HorizontalMerge(self, args: List[str], body: List[str]) -> None:
        if len(args) != 3 or len(body) != 1:
            raise ValueError("HorizontalMerge requires: (T1, T2, is_retained): NewTable")
        t1, t2, is_retained = args
        is_retained = self._parse_bool(is_retained)
        new_table = body[0]
        T1, T2 = self.table(t1), self.table(t2)
        if set(T1["columns"].keys()) != set(T2["columns"].keys()):
            # We enforce identical schema (as per constraint); otherwise refuse.
            raise ValueError("HorizontalMerge requires identical schemas for T1 and T2")
        nt = self.ensure_table(new_table, row_count=int(T1["row_count"]) + int(T2["row_count"]))
        nt["columns"] = deepcopy(T1["columns"])  # identical
        nt["primary_key"] = deepcopy(T1.get("primary_key", []))
        if not is_retained:
            # Replace originals with merged table
            self.meta["tables"].pop(t1, None)
            self.meta["tables"].pop(t2, None)

    # RedundantColumnAdd(SourceTable.Column, TargetTable.NewColumn)
    def _op_RedundantColumnAdd(self, args: List[str], body: List[str]) -> None:
        if len(args) != 2:
            raise ValueError("RedundantColumnAdd requires 2 args: SourceTable.Column, TargetTable.NewColumn")
        src, tgt = args
        st, sc = self._split_qualified(src)
        tt, tc = self._split_qualified(tgt)
        s_col = self.table(st)["columns"].get(sc)
        if s_col is None:
            raise KeyError(f"Source column not found: {src}")
        t_table = self.table(tt)
        if tc in t_table["columns"]:
            raise ValueError(f"Target column already exists: {tt}.{tc}")
        # Copy avg_length; assume not null unless specified via stats hint
        null_frac = float(self.meta.get("stats", {}).get(f"redundant_null_frac:{tt}.{tc}", 0.0))
        t_table["columns"][tc] = {"avg_length": float(s_col.get("avg_length", 0)), "null_frac": null_frac}

    # RedundantColumnDrop(Table.Column)
    def _op_RedundantColumnDrop(self, args: List[str], body: List[str]) -> None:
        if len(args) != 1:
            raise ValueError("RedundantColumnDrop requires 1 arg: Table.Column")
        t, c = self._split_qualified(args[0])
        cols = self.table(t)["columns"]
        if c not in cols:
            raise KeyError(f"Column not found: {t}.{c}")
        cols.pop(c)

    # -------------------------------
    # Utilities
    # -------------------------------
    @staticmethod
    def _parse_bool(s: str) -> bool:
        s = s.strip().lower()
        if s in ("true", "1", "t", "yes", "y"):
            return True
        if s in ("false", "0", "f", "no", "n"):
            return False
        raise ValueError(f"Invalid boolean: {s}")

    @staticmethod
    def _split_qualified(q: str) -> Tuple[str, str]:
        if '.' not in q:
            raise ValueError(f"Expected qualified name Table.Column, got: {q}")
        a, b = q.split('.', 1)
        return a, b

    def _infer_join_rows(self, t1: str, t2: str, k1: str, k2: str) -> int:
        # Try FK inference: if (t2.k2) -> (t1.k1), rows = rows(t2); if (t1.k1)->(t2.k2), rows = rows(t1)
        rows1 = self.table_row_count(t1)
        rows2 = self.table_row_count(t2)
        fk_list = self.meta.get("foreign_keys", [])
        for fk in fk_list:
            if (fk.get("from_table") == t2 and fk.get("to_table") == t1 and
                fk.get("from_columns") == [k2] and fk.get("to_columns") == [k1]):
                return rows2
            if (fk.get("from_table") == t1 and fk.get("to_table") == t2 and
                fk.get("from_columns") == [k1] and fk.get("to_columns") == [k2]):
                return rows1
        # Fallback: assume 1-1 by min cardinality
        return min(rows1, rows2)


def apply_operation(meta: Dict[str, Any], op: str) -> Dict[str, Any]:
    model = StorageModel(meta)
    return model.apply(op)


def main():
    import argparse
    p = argparse.ArgumentParser(description="Apply a schema operation and recalc storage")
    p.add_argument("--input", required=True, help="Input JSON file")
    p.add_argument("--op", required=True, help="Operation string")
    p.add_argument("--output", required=False, help="Output JSON file (writes updated meta)")
    args = p.parse_args()

    with open(args.input, "r", encoding="utf-8") as f:
        meta = json.load(f)

    updated = apply_operation(meta, args.op)

    # Recompute and print brief summary
    total = updated.get("total_storage_bytes")
    if total is None:
        total = StorageModel(updated).compute_total_storage()
        updated["total_storage_bytes"] = total

    # Emit detailed table layout including per-column sizes as requested
    print(json.dumps({
        "total_storage_bytes": total,
        "tables": {
            k: {
                "row_count": v.get("row_count", 0),
                "columns": {
                    cn: {
                        "avg_length": cv.get("avg_length", 0),
                        "null_frac": cv.get("null_frac", 0)
                    } for cn, cv in v.get("columns", {}).items()
                }
            } for k, v in updated.get("tables", {}).items()
        }
    }, ensure_ascii=False, indent=2))

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(updated, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
