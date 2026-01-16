# 基数估计（Cardinality Estimator）

基于数据库 EXPLAIN 的结果，估算过滤（filter）与连接（join）操作的输出行数，用于辅助评估模式（schema）变更前后的存储开销差异。

模块会根据传入的表、列与谓词构造查询，并执行 EXPLAIN 后解析估计行数（当前仅支持 MySQL）：
- 使用 `EXPLAIN FORMAT=JSON`，遍历 `nested_loop` 结构，优先读取 `rows_produced_per_join`，否则退化为 `rows × filtered%`。

## 功能特性
- 过滤基数估计：计算表在应用列谓词后的估计行数。
- 连接基数估计：计算两个表按照连接条件后的估计行数。
- 标识符安全转义（MySQL）；接收列名与谓词参数，自动组装 WHERE/ON 条件。

## 快速开始（MySQL）

```
python LLM-for-DB-Tuning/cardinality/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database mydb \
  filter --table orders --column o_orderstatus --predicate "= 'F'"

python LLM-for-DB-Tuning/cardinality/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database mydb \
  join --left orders --right lineitem --left-col o_orderkey --right-col l_orderkey

# SELECT + GROUP BY/ORDER BY 估计
python LLM-for-DB-Tuning/cardinality/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database mydb \
  select --table lineitem --columns l_orderkey l_partkey \
  --where "l_shipdate >= '1995-01-01'" \
  --group-by l_orderkey \
  --order-by l_orderkey
```

说明：
- `filter` 的 `--predicate` 支持以运算符开头（如 `> 10`），会自动应用到 `--column` 指定的列。
- `join` 默认按 `--left-col = --right-col` 等值连接；如需自定义连接表达式，可使用 `--join-predicate`。

## 参数说明（CLI）
- 通用参数：`--dialect mysql --host --port --user --password --database`
- 过滤：`filter --table <表名> --column <列名> --predicate <谓词>`
- 连接：`join --left <左表> --right <右表> --left-col <左列> --right-col <右列> [--join-predicate <ON 表达式>] [--join-type <连接类型>]`
- 选择：`select --table <表名> [--columns c1 c2 ...] [--where 表达式] [--group-by g1 g2 ...] [--order-by o1 o2 ...]`

## 输出格式
命令行输出 JSON，主要字段：
- `estimated_rows`：最终估计的输出行数
- `dialect`：`mysql`
- `query`：用于 EXPLAIN 的 SQL
- `details`：MySQL 的明细（每个 nested_loop 节点的 rows/filtered/rows_produced_per_join 等）

## 以 Python 方式调用
```
from LLM_for_DB_Tuning.cardinality.sql_builder import build_filter_sql, build_join_sql
from LLM_for_DB_Tuning.cardinality.mysql_explain import MySQLCardinalityEstimator
from DataBase.MySQLDriver import MySQLDriver

drv = MySQLDriver({
    "host": "127.0.0.1", "port": 3306,
    "user": "root", "password": "123456",
    "database": "mydb",
})
drv.connect()

sql = build_filter_sql("orders", "o_orderstatus", "= 'F'")
estimator = MySQLCardinalityEstimator(drv)
result = estimator.estimate(sql)
print(result["estimated_rows"], result["details"])

drv.disconnect()
```

## 注意事项
- 谓词会作为 SQL 片段拼接，请确保输入可信或已在上层完成参数化/校验。
- 当前标识符转义与 SQL 生成遵循 MySQL 语法；如需其他方言请另行扩展。
- 估计值来源于优化器统计信息，仅供近似参考；与实际运行结果可能存在偏差。
