# Database Data Generator

This project is a **SQL-driven synthetic data generator**.  
It parses extended `CREATE TABLE` DDL statements and generates CSV data files that strictly respect:

- Primary key / composite primary key uniqueness
- Foreign key referential integrity
- Column-level constraints and extensions (e.g. RANGE, SET, DECIMAL)
- Statistical distributions (NORMAL, POISSON, HISTOGRAM)
- Controlled data skew (SKEW)

The tool is designed for **benchmarking, testing, and database research scenarios** where realistic, controllable data distributions are required.

---

## Features Overview

### 1. SQL-Driven Schema Definition
Tables and columns are defined entirely in SQL, with additional extensions embedded directly in column definitions.

Supported table-level feature:
- `SIZE = N` — number of rows to generate

Supported column-level features:
- `AUTO_INCREMENT`
- `PRIMARY KEY` / composite primary keys
- `FOREIGN KEY`
- `RANGE(min, max)`
- `SET(k1, k2, ...)`
- `DECIMAL(p, s)`
- `RULER("template$pattern")`
- `SKEW(p)`
- `DISTRI(NORMAL(mu, sigma))`
- `DISTRI(POISSON(lam))`
- `HISTOGRAM({value: ratio, ...})`

---

### 2. Deterministic Global Planning
Before row generation, the system **precomputes global plans** for:

- Distributions (HISTOGRAM / NORMAL / POISSON)
- Skewed columns

This guarantees:
- Exact ratio control for histograms
- Global skew semantics (not row-local randomness)
- No accidental violation of uniqueness constraints

---

### 3. Referential Integrity
- Foreign key columns always sample values from already-generated parent tables
- **Generation order follows SQL table order**
- Skewed foreign keys use existing parent values as hot spots

---

## Project Structure

```text
.
├── generator.py        # Main data generation logic
├── create_table.sql    # Input SQL DDL file
├── output_YYYY-MM-DD_* # Generated CSV output directory
└── README.md
