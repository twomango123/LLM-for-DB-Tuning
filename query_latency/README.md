# 查询延迟收集器

本工具会递归扫描指定目录下的 `.sql` 文件，逐条在 MySQL 数据库中执行并记录每个查询的耗时。查询 ID 按文件名中的 `queryN` 提取（例如 `query62.sql` → `query62`），便于后续按 ID 对齐构造 Prompt。

## 文件说明
- `collect_latency.py` — 主脚本，执行查询并记录延迟（调用项目内 `DataBase/MySQLDriver.py`）
- `db_config.ini` — 数据库连接参数（请填入真实的 `user/password/database`）
- `latency_results.csv` — 成功结果，格式：`query_id,elapsed_ms`
- `latency_errors.csv` — 失败结果，格式：`query_id,error`
- `test_connection.py` — 独立的数据库连通性测试脚本

## 环境要求
- Python 3.8+
- 依赖：`mysql-connector-python`

安装：
```
pip install mysql-connector-python
```

## 快速开始
1) 编辑数据库配置：`LLM-for-DB-Tuning/query_latency/db_config.ini`
2) 先测试连接：
```
python3 LLM-for-DB-Tuning/query_latency/test_connection.py \
  --config LLM-for-DB-Tuning/query_latency/db_config.ini
```
3) 运行延迟收集（示例扫描 Spider 的 `csu_1` 目录）：
```
python3 LLM-for-DB-Tuning/query_latency/collect_latency.py \
  --sql-dir spider_data/spider_data/database_mysql/csu_1 \
  --config LLM-for-DB-Tuning/query_latency/db_config.ini \
  --output LLM-for-DB-Tuning/query_latency/latency_results.csv \
  --error-output LLM-for-DB-Tuning/query_latency/latency_errors.csv
```

## 参数说明
- `--sql-dir`：待执行的 `.sql` 文件所在目录（递归扫描）
- `--config`：INI 配置文件路径（必须包含 `[mysql]` 段）
- `--output`：成功结果 CSV 输出路径（默认同目录 `latency_results.csv`）
- `--error-output`：失败结果 CSV 输出路径（默认同目录 `latency_errors.csv`）

## 行为说明
- 只执行每个文件中第一条非空语句（分号 `;` 之前）；没有分号则执行整个文件内容。
- 自动提取 `queryN` 作为 `query_id`；若无法匹配，则使用文件名（不含扩展名）。
- SELECT/EXPLAIN 等查询会取回所有结果，确保耗时覆盖结果读取；其他语句使用非查询接口执行。
- 脚本通过项目内 `MySQLDriver` 建立连接与执行，与你的其他模块保持一致。

## 常见问题
- 使用口令：请在 `db_config.ini` 的 `password` 行填入真实口令，避免出现 “using password: NO”。不要在密码两侧添加引号或在同一行追加注释。
- 账号主机匹配：`host = localhost` 时要求 MySQL 中存在 `user@localhost`；如使用 `127.0.0.1`，需有 `user@127.0.0.1` 或 `user@'%'`。

