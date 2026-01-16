from __future__ import annotations

from typing import Optional, Iterable


def quote_ident_mysql(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def qualify_mysql(table: str, column: Optional[str] = None) -> str:
    if column is None:
        return quote_ident_mysql(table)
    return f"{quote_ident_mysql(table)}.{quote_ident_mysql(column)}"


def build_filter_sql(table: str, column: str, predicate: str, limit: Optional[int] = None) -> str:
    """Build a SELECT for filtering a table by column predicate.

    `predicate` may be either a full SQL expression or start with an operator
    like "> 10". In the latter case, it will be prefixed with the qualified
    column name.
    """
    pred = predicate.strip()
    lhs = qualify_mysql(table, column)
    if pred and pred[0] in "=<>!":
        where = f"{lhs} {pred}"
    else:
        where = pred if lhs in pred else f"{lhs} {pred}"
    sql = f"SELECT * FROM {quote_ident_mysql(table)} WHERE {where}"
    if limit is not None and limit > 0:
        sql += f" LIMIT {int(limit)}"
    return sql


def build_join_sql(
    left: str,
    right: str,
    left_col: str,
    right_col: str,
    join_predicate: Optional[str] = None,
    join_type: str = "INNER",
    limit: Optional[int] = None,
) -> str:
    """Build a basic join query used for EXPLAIN cardinality checks.

    When `join_predicate` is None, uses equality between the provided columns.
    """
    lq = quote_ident_mysql(left)
    rq = quote_ident_mysql(right)
    on = join_predicate or (
        f"{qualify_mysql(left, left_col)} = {qualify_mysql(right, right_col)}"
    )
    sql = f"SELECT * FROM {lq} {join_type} JOIN {rq} ON {on}"
    if limit is not None and limit > 0:
        sql += f" LIMIT {int(limit)}"
    return sql


def build_select_sql(
    table: str,
    columns: Optional[Iterable[str]] = None,
    where: Optional[str] = None,
    group_by: Optional[Iterable[str]] = None,
    order_by: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
) -> str:
    """构造包含 SELECT/GROUP BY/ORDER BY 的查询（用于 EXPLAIN）。

    - columns: 需要选择的列名（不传则为 *）。
    - where: 过滤谓词字符串（将直接拼接到 WHERE）。
    - group_by/order_by: 传入列名集合，内部做标识符转义；如需表达式请直接传入 where/自定义 SQL。
    """
    if columns:
        cols = ", ".join(quote_ident_mysql(c) for c in columns)
    else:
        cols = "*"

    sql = f"SELECT {cols} FROM {quote_ident_mysql(table)}"
    if where:
        sql += f" WHERE {where}"
    if group_by:
        gb = ", ".join(quote_ident_mysql(c) for c in group_by)
        sql += f" GROUP BY {gb}"
    if order_by:
        ob = ", ".join(quote_ident_mysql(c) for c in order_by)
        sql += f" ORDER BY {ob}"
    if limit is not None and limit > 0:
        sql += f" LIMIT {int(limit)}"
    return sql
