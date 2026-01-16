# 存储开销估算（列级）

按表/列估算数据存储字节数（仅表数据部分）：
- 定长类型：通过已知字节大小 × 估计行数。
- 变长类型：按给定抽样比例进行抽样，使用函数统计样本字节数，除以抽样比例近似全列字节数。

支持 MySQL：
- 抽样字节：`OCTET_LENGTH(col)` + `RAND()`
- 行数估计：`EXPLAIN FORMAT=JSON`

输出格式：JSON Lines（每行一个列记录），利于后续快速扫描或以流式方式读取。

## 快速开始（MySQL）

```
python LLM-for-DB-Tuning/storage/cli.py \
  --dialect mysql \
  --host 127.0.0.1 --port 3306 \
  --user root --password 123456 --database mydb \
  --sample-ratio 0.01 \
  --min-sample-rows 100 \
  --output out/column_storage.jsonl
```

可选：限定表
```
  --tables orders lineitem nation
```

## 输出字段（JSONL）
- `schema`（MySQL 固定为当前库）/`table`/`column`/`data_type`
- `is_fixed`：是否为定长估计
- `bytes_per_value`：定长字节数（如适用）
- `sample_ratio`/`sample_row_count`/`sample_total_bytes`/`avg_bytes_per_value`（抽样信息，变长列）
- `estimated_row_count`：表估计行数（来自 EXPLAIN 或统计）
- `estimated_total_bytes`：列估算总字节
- `method`：`fixed_length` 或 `sample_estimate`

## 注意
- 抽样为近似估计；结果依赖统计信息与样本命中情况。`--min-sample-rows` 可降低抽样误差。
- 定长映射采用通用近似；如需严谨估算（例如 numeric/decimal 可变长度、char(n) 多字节字符），建议转为抽样或按业务定制映射。
- 输出为 JSON Lines，适合后续用 Python/Pandas/Spark 等工具读取聚合。
