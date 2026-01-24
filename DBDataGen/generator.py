import ast
import csv
import random
import re
import string
from collections import OrderedDict
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_DOWN, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import numpy as np

CURRENT_TIME: str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Default date range for DATE columns
START_DATE: date = date(1999, 7, 10)
END_DATE: date = date(2099, 12, 31)

# Default string length for STRING columns
STRING_MIN_LENGTH: int = 1
STRING_MAX_LENGTH: int = 100


class Column:
    """
    Column metadata parsed from a CREATE TABLE statement line.

    This class extracts and stores column constraints / extensions embedded in the
    column definition line, such as:
    - VARCHAR/CHAR length
    - SKEW(p)
    - DECIMAL(p,s)
    - Distribution spec: DISTRI(NORMAL(mu,sigma)), DISTRI(POISSON(lam)), HISTOGRAM({...})
    """

    def __init__(self, name: str, col_type: str, attrs: str) -> None:
        """
        Initialize a Column.

        Args:
            name: Column name.
            col_type: SQL column type (e.g., INT, VARCHAR, DECIMAL).
            attrs: The full column definition line containing constraints and extensions
                   (e.g., AUTO_INCREMENT, RANGE, SET, RULER, SKEW, DISTRI, HISTOGRAM).
        """
        self.name: str = name
        self.type: str = col_type.upper()
        self.attrs: str = attrs  # AUTO_INCREMENT, SKEW, RANGE, SET, RULER, NOT NULL, ...
        self.max_length: Optional[int] = self._parse_length()
        self.skew_p: Optional[float] = self._parse_skew()  # SKEW(p)
        self.decimal_spec: Optional[Tuple[int, int]] = self._parse_decimal()  # DECIMAL(p,s)
        self.distribution: Optional[Dict[str, Any]] = self._parse_distribution()

    def _parse_length(self) -> Optional[int]:
        """Parse VARCHAR/CHAR length from the attribute line."""
        m = re.search(r"(VARCHAR|CHAR)\s*\((\d+)\)", self.attrs, re.I)
        return int(m.group(2)) if m else None

    def _parse_skew(self) -> Optional[float]:
        """Parse SKEW(p) value from the attribute line."""
        m = re.search(r"SKEW\(([\d.]+)\)", self.attrs)
        return float(m.group(1)) if m else None

    def _parse_decimal(self) -> Optional[Tuple[int, int]]:
        """
        Parse DECIMAL(p,s) specification.

        Returns:
            A tuple (p, s) where p is precision and s is scale, or None if not present.

        Raises:
            ValueError: If DECIMAL(p,s) is invalid (e.g., s >= p).
        """
        m = re.search(r"DECIMAL\s*\((\d+)\s*,\s*(\d+)\)", self.attrs, re.I)
        if m:
            p, s = int(m.group(1)), int(m.group(2))
            if s >= p:
                raise ValueError(f"Invalid DECIMAL({p},{s}) on column {self.name}")
            return p, s
        return None

    def _parse_distribution(self) -> Optional[Dict[str, Any]]:
        """
        Parse distribution specification.

        Supported:
        - HISTOGRAM({value: ratio, ...})
        - DISTRI(NORMAL(mu,sigma))
        - DISTRI(POISSON(lam))

        Returns:
            A dict describing the distribution, or None if not present.
        """
        # HISTOGRAM
        m = re.search(r"HISTOGRAM\s*\((\{.*?\})\)", self.attrs, re.I)
        if m:
            raw = ast.literal_eval(m.group(1))
            return {
                "type": "histogram",
                "weights": {k: float(v) for k, v in raw.items()},
            }

        # DISTRIBUTION
        m = re.search(r"DISTRI\s*\(\s*(\w+)\((.*?)\)\s*\)", self.attrs, re.I)
        if m:
            name = m.group(1).lower()
            params = [float(x) for x in m.group(2).split(",")]

            if name == "normal":
                return {"type": "normal", "mu": params[0], "sigma": params[1]}
            if name == "poisson":
                return {"type": "poisson", "lam": params[0]}

        return None


class Table:
    """
    Table metadata parsed from a CREATE TABLE block.

    Attributes:
        name: Table name.
        size: Number of rows to generate (SIZE=... in the DDL block).
        columns: Ordered mapping of column name -> Column.
        primary_key: List of primary key column names.
        foreign_keys: List of foreign key tuples (col, ref_table, ref_col).
    """

    def __init__(self, name: str, size: int) -> None:
        """
        Initialize a Table.

        Args:
            name: Table name.
            size: Number of rows to generate for the table.
        """
        self.name: str = name
        self.size: int = size
        self.columns: "OrderedDict[str, Column]" = OrderedDict()
        self.primary_key: List[str] = []
        self.foreign_keys: List[Tuple[str, str, str]] = []  # (col, ref_table, ref_col)


def parse_sql(sql_text: str) -> "OrderedDict[str, Table]":
    """
    Parse SQL text containing one or more CREATE TABLE blocks with SIZE annotations.

    Expected DDL pattern:
        CREATE TABLE `table_name` (
            ...
        ) ... SIZE = <N>;

    Args:
        sql_text: Full SQL file content.

    Returns:
        Ordered mapping: table_name -> Table object.
    """
    tables: "OrderedDict[str, Table]" = OrderedDict()
    blocks = re.findall(r"CREATE TABLE.*?SIZE\s*=\s*\d+;", sql_text, re.S)

    for block in blocks:
        table_name_match = re.search(r"CREATE TABLE\s+`(\w+)`", block)
        size_match = re.search(r"SIZE\s*=\s*(\d+)", block)
        if table_name_match is None or size_match is None:
            continue

        table_name = table_name_match.group(1)
        size = int(size_match.group(1))
        table = Table(table_name, size)

        # Columns
        for line in block.splitlines():
            col_match = re.match(r"\s*`(\w+)`\s+(\w+)", line)
            if col_match:
                col, ctype = col_match.groups()
                table.columns[col] = Column(col, ctype.upper(), line)

        # Primary key
        pk = re.search(r"PRIMARY KEY\s*\((.*?)\)", block)
        if pk:
            table.primary_key = [x.strip(" `") for x in pk.group(1).split(",")]

        # Foreign keys
        for fk in re.finditer(
            r"FOREIGN KEY\s*\(`(\w+)`\)\s+REFERENCES\s+`(\w+)`\s*\(`(\w+)`\)",
            block,
        ):
            table.foreign_keys.append(fk.groups())  # type: ignore[arg-type]

        tables[table_name] = table

    return tables


def rand_string(min_len: int = STRING_MIN_LENGTH, max_len: int = STRING_MAX_LENGTH) -> str:
    """
    Generate a random ASCII letters string.

    Args:
        min_len: Minimum string length.
        max_len: Maximum string length.

    Returns:
        A random string. Returns an empty string if the length range is invalid.
    """
    if min_len < 0 or max_len < min_len or max_len == 0:
        return ""
    length = random.randint(min_len, max_len)
    return "".join(random.choices(string.ascii_letters, k=length))


def rand_date(start: date = START_DATE, end: date = END_DATE) -> date:
    """
    Generate a random date within [start, end].

    Args:
        start: Start date (inclusive).
        end: End date (inclusive).

    Returns:
        A random date.
    """
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def apply_ruler(rule: str, max_len: int) -> str:
    """
    Apply a RULER template to generate a string.

    The placeholder '$' will be replaced by a random string so that the resulting
    length does not exceed max_len.

    Example:
        rule = "$@email.com"

    Args:
        rule: RULER template string, where '$' indicates the variable part.
        max_len: Maximum total length of the produced string.

    Returns:
        The generated string after placeholder substitution.
    """
    fixed_len = len(rule.replace("$", ""))
    remain = max_len - fixed_len
    if remain <= 0:
        return rule.replace("$", "")
    return rule.replace("$", rand_string(max_len=remain))


def build_distribution_plans(
    table: Table,
    all_data: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, List[Any]]:
    """
    Pre-compute per-column values for columns with distribution specifications.

    Supported:
    - HISTOGRAM: allocate explicit values by ratios, then fill the remaining with random values
    - NORMAL: sample from normal distribution
    - POISSON: sample from Poisson distribution

    Notes:
        Single-column primary keys are excluded from distribution planning to avoid violating uniqueness.

    Args:
        table: Table metadata.
        all_data: Already generated data for other tables (used for FK-related logic elsewhere).

    Returns:
        Mapping: column_name -> list of planned values (length equals table.size).
    """
    plans: Dict[str, List[Any]] = {}
    n_rows = table.size

    for col_name, col in table.columns.items():
        dist = col.distribution
        if not dist:
            continue

        # Disallow distribution planning on a single-column primary key to preserve uniqueness.
        if col_name in table.primary_key and len(table.primary_key) == 1:
            continue

        # ================= HISTOGRAM =================
        if dist["type"] == "histogram":
            weights: Dict[Any, float] = dist["weights"]
            explicit_values: List[Any] = []
            used_count = 0

            # 1) Allocate explicit values by ratio
            for value, ratio in weights.items():
                c = int(round(ratio * n_rows))
                if c <= 0:
                    continue
                explicit_values.extend([value] * c)
                used_count += c

            # 2) Remaining rows
            remaining = n_rows - used_count
            if remaining < 0:
                raise ValueError(
                    f"HISTOGRAM ratios exceed 1.0 on {table.name}.{col_name}"
                )

            # 3) Fill remaining with generated values not overlapping explicit keys
            histogram_keys = set(weights.keys())
            fallback_values: List[Any] = []
            for _ in range(remaining):
                v = generate_base_value(col)
                while v in histogram_keys:
                    v = generate_base_value(col)
                fallback_values.append(v)

            values = explicit_values + fallback_values
            random.shuffle(values)
            plans[col_name] = values
            continue

        # ================= NORMAL =================
        if dist["type"] == "normal":
            mu, sigma = float(dist["mu"]), float(dist["sigma"])
            samples = np.random.normal(mu, sigma, n_rows)
            plans[col_name] = list(samples)
            continue

        # ================= POISSON =================
        if dist["type"] == "poisson":
            lam = float(dist["lam"])
            samples = np.random.poisson(lam, n_rows)
            plans[col_name] = list(samples)
            continue

    return plans


def generate_decimal_value(
    precision: int,
    scale: int,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
) -> Decimal:
    """
    Generate a Decimal value conforming to DECIMAL(precision, scale).

    Args:
        precision: Total number of digits (p).
        scale: Number of fractional digits (s).
        min_value: Optional lower bound for the integer part.
        max_value: Optional upper bound for the integer part.

    Returns:
        A quantized Decimal value with the requested scale.
    """
    int_digits = precision - scale
    max_int = 10**int_digits - 1

    lo = float(min_value) if min_value is not None else 0.0
    hi = float(max_value) if max_value is not None else float(max_int)
    if hi > max_int:
        hi = float(max_int)

    integer_part = random.randint(int(lo), int(hi))
    frac_part = random.randint(0, 10**scale - 1)

    value = Decimal(f"{integer_part}.{frac_part:0{scale}d}")
    getcontext().prec = precision
    return value.quantize(Decimal(f"1.{'0' * scale}"), rounding=ROUND_DOWN)


def generate_base_value(col: Column) -> Any:
    """
    Generate a base value for a column according to its constraints.

    Supported constraints / extensions:
    - RANGE(min,max) for numeric types (INT/FLOAT/DOUBLE/DECIMAL)
    - DECIMAL(p,s)
    - SET(...)
    - RULER("...$...")
    - VARCHAR/CHAR/TEXT/BLOB random strings
    - DATE random date (ISO format string)

    Args:
        col: Column metadata.

    Returns:
        A generated value. For DECIMAL, returns a Decimal (caller may convert to string).
    """
    line = col.attrs

    # ---------- RANGE ----------
    rng = re.search(r"RANGE\(([-\d.]+),\s*([-\d.]+)\)", line)

    # ---------- DECIMAL ----------
    if col.decimal_spec:
        precision, scale = col.decimal_spec
        min_v: Optional[float] = None
        max_v: Optional[float] = None
        if rng:
            min_v, max_v = float(rng.group(1)), float(rng.group(2))
        return generate_decimal_value(precision, scale, min_v, max_v)

    # ---------- FLOAT / DOUBLE ----------
    if col.type in {"FLOAT", "DOUBLE"}:
        min_v, max_v = (0.0, 1000.0)
        if rng:
            min_v, max_v = float(rng.group(1)), float(rng.group(2))
        val = random.uniform(min_v, max_v)
        return round(val, 6 if col.type == "FLOAT" else 10)

    # ---------- INT ----------
    if col.type.startswith("INT"):
        if rng:
            return random.randint(int(float(rng.group(1))), int(float(rng.group(2))))
        return random.randint(1, 10000)

    # ---------- SET ----------
    st = re.search(r"SET\((.*?)\)", line)
    if st:
        return random.choice([x.strip(" '") for x in st.group(1).split(",")])

    # ---------- RULER ----------
    ruler = re.search(r'RULER\("(.+?)"\)', line)
    if ruler:
        rule = ruler.group(1)
        fixed_len = len(rule.replace("$", ""))
        max_len = col.max_length if col.max_length is not None else fixed_len + 8
        remain = max_len - fixed_len
        if remain <= 0:
            return rule.replace("$", "")
        return rule.replace("$", rand_string(max_len=remain))

    # ---------- STRING ----------
    if col.type.startswith(("VARCHAR", "CHAR")):
        max_len = col.max_length if col.max_length is not None else 10
        return rand_string(max_len=max_len)
    if col.type.startswith(("TEXT", "BLOB")):
        return rand_string()

    # ---------- DATE ----------
    if col.type == "DATE":
        return rand_date().isoformat()

    return None


def build_skew_plans(
    table: Table,
    all_data: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Dict[str, Any]]:
    """
    Build skew plans for columns marked with SKEW(p).

    A skew plan indicates which row indices should receive a repeated "hot" value.

    Notes:
        - Primary key columns are excluded to preserve uniqueness.
        - For foreign key columns, the hot value is sampled from the referenced parent table.
        - For non-foreign-key columns, the hot value is generated via generate_base_value().

    Args:
        table: Table metadata.
        all_data: Generated rows for previously generated tables.

    Returns:
        Mapping: col_name -> {"idx_set": set(row_indices), "hot": hot_value}
    """
    plans: Dict[str, Dict[str, Any]] = {}
    n_rows = table.size

    for col_name, col in table.columns.items():
        p = col.skew_p
        if p is None:
            continue

        # Primary key columns cannot be skewed (would violate uniqueness).
        if col_name in table.primary_key:
            continue

        # Compute the number of rows to skew (strictly bounded).
        k = int(round(p * n_rows))
        k = max(0, min(n_rows, k))
        if k == 0:
            continue

        idx_set: Set[int] = set(random.sample(range(n_rows), k))

        # Choose the hot value: for FK columns, sample an existing parent value.
        fk_info = next((fk for fk in table.foreign_keys if fk[0] == col_name), None)
        if fk_info:
            _, parent_table, parent_col = fk_info
            parent_rows = all_data.get(parent_table, [])
            if not parent_rows:
                raise ValueError(
                    f"Skew FK column {table.name}.{col_name} requires parent table "
                    f"{parent_table} generated first."
                )
            hot = random.choice(parent_rows)[parent_col]
        else:
            hot = generate_base_value(col)

        plans[col_name] = {"idx_set": idx_set, "hot": hot}

    return plans


def generate_table(
    table: Table,
    all_data: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    """
    Generate rows for a single table.

    Guarantees (best-effort with bounded retries):
    - Primary key / composite primary key uniqueness
    - Foreign key referential integrity (samples from existing parent rows)
    - Global plan semantics for HISTOGRAM / DISTRI / SKEW

    Args:
        table: Table metadata.
        all_data: Generated data for previously generated tables (used for FK lookups).

    Returns:
        List of row dicts.

    Raises:
        RuntimeError: If unable to generate the required number of unique rows within
                      the attempt limit.
    """
    rows: List[Dict[str, Any]] = []
    pk_seen: Set[Tuple[Any, ...]] = set()
    auto_inc = 0

    # ===================== Precomputed global plans =====================
    distribution_plans = build_distribution_plans(table, all_data)
    skew_plans = build_skew_plans(table, all_data)

    n_rows = table.size
    row_index = 0
    attempt = 0
    max_attempts = n_rows * 10  # Prevent infinite loops

    # ===================== Main loop =====================
    while len(rows) < n_rows and attempt < max_attempts:
        attempt += 1
        row: Dict[str, Any] = {}

        for col_name, col in table.columns.items():
            # ---------- AUTO_INCREMENT ----------
            if "AUTO_INCREMENT" in col.attrs:
                row[col_name] = auto_inc
                auto_inc += 1
                continue

            # ---------- DISTRIBUTION (HISTOGRAM / NORMAL / POISSON) ----------
            if col_name in distribution_plans:
                raw: Any = distribution_plans[col_name][row_index]

                # RANGE clipping
                rng = re.search(r"RANGE\(([-\d.]+),\s*([-\d.]+)\)", col.attrs)
                if rng:
                    lo, hi = float(rng.group(1)), float(rng.group(2))
                    raw = min(max(raw, lo), hi)

                # SET mapping (by index)
                st = re.search(r"SET\((.*?)\)", col.attrs)
                if st:
                    options = [x.strip(" '") for x in st.group(1).split(",")]
                    raw = options[int(abs(raw)) % len(options)]

                # String truncation
                if col.type.startswith(("VARCHAR", "CHAR")) and col.max_length is not None:
                    raw = str(raw)[: col.max_length]

                # DECIMAL is serialized as string for CSV
                if col.decimal_spec:
                    raw = str(raw)

                row[col_name] = raw
                continue

            # ---------- SKEW ----------
            if col_name in skew_plans and row_index in skew_plans[col_name]["idx_set"]:
                row[col_name] = skew_plans[col_name]["hot"]
                continue

            # ---------- FOREIGN KEY ----------
            fk_info = next((fk for fk in table.foreign_keys if fk[0] == col_name), None)
            if fk_info:
                _, parent_table, parent_col = fk_info
                parent_rows = all_data[parent_table]
                row[col_name] = random.choice(parent_rows)[parent_col]
                continue

            # ---------- BASE GENERATOR ----------
            value = generate_base_value(col)

            # DECIMAL is serialized as string for CSV
            if col.decimal_spec:
                value = str(value)

            row[col_name] = value

        # ===================== Primary key check =====================
        if table.primary_key:
            pk = tuple(row[k] for k in table.primary_key)
            if pk in pk_seen:
                continue
            pk_seen.add(pk)

        rows.append(row)
        row_index += 1

    # ===================== Failure guard =====================
    if len(rows) < n_rows:
        raise RuntimeError(
            f"Failed to generate {n_rows} unique rows for table {table.name}. "
            f"Generated {len(rows)} rows. "
            f"Possible reasons: excessive skew, tight PK constraints."
        )

    return rows


def generate_csv(sql_file: Union[str, Path], out_dir: Optional[Union[str, Path]] = None) -> None:
    """
    Generate CSV files for all tables defined in the SQL file.

    Args:
        sql_file: Path to the SQL file containing CREATE TABLE blocks with SIZE annotations.
        out_dir: Output directory path. If None, a timestamped folder will be used.

    Returns:
        None
    """
    sql_path = Path(sql_file)
    sql_text = sql_path.read_text(encoding="utf8")

    tables = parse_sql(sql_text)

    output_dir = Path(out_dir) if out_dir is not None else Path(f"output_{CURRENT_TIME}")
    output_dir.mkdir(exist_ok=True)

    all_data: Dict[str, List[Dict[str, Any]]] = {}

    for name, table in tables.items():
        data = generate_table(table, all_data)
        all_data[name] = data

        with open(output_dir / f"{name}.csv", "w", newline="", encoding="utf8") as f:
            writer = csv.DictWriter(f, fieldnames=table.columns.keys())
            writer.writeheader()
            writer.writerows(data)

        print(f"[Done] {name}.csv generated ({len(data)} rows)")

    print("Have a nice day!")

if __name__ == "__main__":
    generate_csv("create_table.sql")