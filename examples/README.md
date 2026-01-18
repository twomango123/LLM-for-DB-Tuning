# Cardinality 与 Storage 功能测试示例

本目录提供两类可直接运行的测试示例：
- 端到端：在真实 MySQL 上导入极小示例数据后，运行 `cardinality/cli.py` 与 `storage/cli.py`。
- 无数据库：使用内置的 Mock 驱动，快速验证核心逻辑与输出结构。

注意：以下命令均以仓库根目录为当前目录执行。

## 一、端到端（真实 MySQL）

1) 导入示例数据（会创建数据库 `codex_demo` 并写入两张表）

```
mysql -uroot -p -h127.0.0.1 -P3306 < LLM-for-DB-Tuning/examples/mysql_demo.sql
```

2) Cardinality 基数估计示例

```
# 过滤基数（按订单状态 = 'F'）
python LLM-for-DB-Tuning/cardinality/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database codex_demo \
  filter --table orders --column o_orderstatus --predicate "= 'F'"

# 连接基数（orders.o_orderkey = lineitem.l_orderkey）
python LLM-for-DB-Tuning/cardinality/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database codex_demo \
  join --left orders --right lineitem --left-col o_orderkey --right-col l_orderkey

# SELECT + GROUP BY/ORDER BY（可选）
python LLM-for-DB-Tuning/cardinality/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database codex_demo \
  select --table lineitem --columns l_orderkey l_partkey \
  --where "l_shipdate >= '1995-01-01'" \
  --group-by l_orderkey \
  --order-by l_orderkey
```

3) Storage 列级存储开销估算示例

```
python LLM-for-DB-Tuning/storage/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database codex_demo \
  --tables orders lineitem \
  --sample-ratio 0.5 \
  --min-sample-rows 1 \
  --output LLM-for-DB-Tuning/output_dir/column_storage.jsonl

# 快速查看输出前几行
head -n 10 LLM-for-DB-Tuning/output_dir/column_storage.jsonl
```

4) 聚合查看每张表估算总字节（可选）

```
python - <<'PY'
import json, collections, pathlib
p = pathlib.Path('LLM-for-DB-Tuning/output_dir/column_storage.jsonl')
agg = collections.defaultdict(int)
for ln in p.read_text(encoding='utf-8').splitlines():
    rec = json.loads(ln)
    b = rec.get('estimated_total_bytes')
    if isinstance(b, int):
        agg[rec['table']] += b
print(dict(agg))
PY
```

提示：MySQL 的 EXPLAIN 估计值依赖统计信息，输出的 `estimated_rows` 与真实行数可能不同，示例用于功能验证即可。

## 二、无需数据库（Mock 测试）

直接运行以下脚本，会使用内置的 Stub 驱动模拟 `EXPLAIN` 与抽样统计，适合在无数据库环境下快速验证：

```
python LLM-for-DB-Tuning/examples/test_cardinality_mock.py
python LLM-for-DB-Tuning/examples/test_storage_mock.py
```

两者会打印：
- 构造的 SQL 片段（便于核对）
- 解析出的估计结果（`estimated_rows` / 列级 `estimated_total_bytes` 等）

