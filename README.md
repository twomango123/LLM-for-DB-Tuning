# rewrite接口 测试运行方法  
在当前目录创建并激活虚拟环境  
`python3 -m venv venv`  
`source venv/bin/activate`  
`pip install -r requirements.txt`    

进入rewrite目录    
`cd rewrite`  
可运行示例测试  
`python3 test_rewrite.py`  


# L0:evaluation-chbenchmark
### 创建镜像 运行并进入docker  
在git clone之前需要先创建一个工作目录，在工作目录下clone  
```bash
chmod +x docker.sh  
./docker.sh  
```   
### 进入docker后 启动mysql服务  
`service mysql start`  

# 真实数据集测试   
### 准备历史查询  
python3 spider_data/spider_data/export_train_queries.py --input spider_data/spider_data/train_spider.json --db-root spider_data/spider_data/database_mysql  
### 扩充数据集 1w->5w  13个表
python3 Data/schema_data.py output_dir/Actual_Order_Products.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Actual_Orders.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Addresses.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Customer_Addresses.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Customers.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Delivery_Route_Locations.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Delivery_Routes.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Employees.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Order_Deliveries.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Products.csv 10 output_dir/
-Order_Products
python3 Data/schema_data.py output_dir/Regular_Order_Products.csv 10 output_dir/

python3 Data/schema_data.py output_dir/Regular_Orders.csv 10 output_dir/  

python3 Data/schema_data.py output_dir/Trucks.csv 10 output_dir/  

### 将数据集导入数据库  
`cp -r output_dir /var/lib/mysql-files/output_dir`
`cp -r output_dir/rewritten /var/lib/mysql-files/output_dir/query`
`mysql -u root -p tpcch`  
`CREATE DATABASE customer_deliveries;`  
`USE customer_deliveries;`
建表schema  
`SOURCE /LLM-for-DB-Tuning/scripts/schema.sql;` 

填csv数据  
见INSERT_SQL.sql
```SQL

SET FOREIGN_KEY_CHECKS=0;

LOAD DATA INFILE '/var/lib/mysql-files/output_dir/Products_SF_5.csv' INTO TABLE Products CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES;

SET FOREIGN_KEY_CHECKS=1;
```  



### 测试历史查询延迟  
```python
python3 query_latency/collect_latency.py \
  --sql-dir output_dir  \
  --config query_latency/db_config.ini \
  --output query_latency/latency_results.csv \
  --error-output query_latency/latency_errors.csv
```   

示例：  
改写前query51：  
```sql
SELECT faculty FROM (SELECT * FROM Campuses_faculty_losangeles UNION ALL SELECT * FROM Campuses_faculty_others) AS T1 JOIN campuses AS T2 ON T1.campus = T2.id WHERE T1.year = 2002 AND T2.campus = "Long Beach State University";
``` 

改写后：
```sql  
SELECT faculty FROM faculty AS T1 JOIN campuses AS T2 ON T1.campus  =  T2.id WHERE T1.year  =  2002 AND T2.campus  =  "Long Beach State University";  
```  


### 根据当前数据库信息构造提示词  
```python  
python3 prompt/COMBINATION.py --schema-sql output_dir/schema/schema.sql --csv-dir output_dir/ --sql-dir output_dir --latency query_latency/latency_results.csv --out prompt/final_prompt.md
``` 

python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/output_dir/schema/schema.sql --csv-dir LLM-for-DB-Tuning/dbdatagen/output_2026-01-20_03-13-05 --sql-dir /var/lib/mysql-files/output_dir/query --out LLM-for-DB-Tuning/prompt/final_prompt.md

### tpch构造提示词
EXEC_COUNTS=LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update/sample_execution_counts_chbench.csv python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/DataBase/cleaned_sql/schema.sql --csv-dir /var/lib/mysql-files --sql-dir LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update --out LLM-for-DB-Tuning/prompt/final_prompt.md

PART2_DEBUG=1 EXEC_COUNTS=LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update/sample_execution_counts_chbench.csv DB_CONFIG=LLM-for-DB-Tuning/query_latency/db_config.ini python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/DataBase/cleaned_sql/schema.sql --csv-dir /var/lib/mysql-files --sql-dir LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update --out LLM-for-DB-Tuning/prompt/final_prompt.md
86e5fc9c7950
cat prompt/final_prompt.md
### TPCH构造  
`cp ../DataBase/cleaned_sql/schema.sql /var/lib/mysql-files/schema.sql`  
`mv /var/lib/mysql-files/order.tbl /var/lib/mysql-files/orders.tbl`

```python 
python3 prompt/COMBINATION.py --schema-sql /var/lib/mysql-files/schema.sql --csv-dir /var/lib/mysql-files --sql-dir DataBase/cleaned_sql/query_sql --latency /var/lib/mysql-files/latency_AP.txt --out prompt/final_prompt.md
```  

### 使用提示词与LLM对话 得到操作序列  
提示词路径
`cat prompt/final_prompt.md`


### 解析操作序列，调用改写功能  
```
python3 response/runner.py --use-db --host localhost --port 3306 --user root --password '123!@#200' --database tpcch
```

```
python3 response/runner.py --use-db --host localhost --port 3306 --user root --password '123!@#200' --database tpcch --sql-dir DataBase/cleaned_sql/query_sql --out-sql-dir response/rewritten_sql
```

1) Table Join → 创建 orderline_orders
CREATE TABLE tpcch.orderline_orders AS SELECT ol.ol_o_id, ol.ol_w_id, ol.ol_d_id, ol.ol_number, ol.ol_i_id, ol.ol_supply_w_id, ol.ol_delivery_d, ol.ol_quantity, ol.ol_amount, ol.ol_dist_info, o.o_entry_d, o.o_carrier_id, o.o_ol_cnt FROM tpcch.orderline AS ol JOIN tpcch.orders AS o ON o.o_w_id = ol.ol_w_id AND o.o_d_id = ol.ol_d_id AND o.o_id = ol.ol_o_id;

2) Vertical Split → 创建 frequent / infrequent

推荐（包含 ol_number 作为主键列，避免丢行/聚合歧义）:
CREATE TABLE tpcch.orderline_orders_frequent AS SELECT DISTINCT ol_o_id, ol_w_id, ol_d_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, o_entry_d, o_carrier_id, o_ol_cnt FROM tpcch.orderline_orders;
CREATE TABLE tpcch.orderline_orders_infrequent AS SELECT DISTINCT ol_o_id, ol_w_id, ol_d_id, ol_number, ol_dist_info FROM tpcch.orderline_orders;

### 测试改写后查询延迟   
```python3 query_latency/collect_latency.py \
  --sql-dir output_dir/rewritten \
  --config query_latency/db_config.ini \
  --output query_latency/new_results.csv \
  --error-output query_latency/new_errors.csv
  ```

# 单独运行ch-benchmark （在原始统计tpms、qps基础上加入latency统计）
### 进入docker后 启动mysql服务  
`service mysql start`  
### 进入ch-benchmark 编译
```bash
cd ch-benchmark  
make  
```  
./chBenchmark -csv -wh 1 -pa /var/lib/mysql-files
`mysql -u root -p tpcch`
ALTER TABLE table_name 
RENAME COLUMN old_column_name TO new_column_name;

SELECT 
    COLUMN_NAME
FROM 
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE 
    TABLE_SCHEMA = 'tpcch' 
    AND TABLE_NAME = 'nation'
    AND CONSTRAINT_NAME = 'PRIMARY';
### 运行生成数据命令 可指定warehouse数量（-wh 1） 最好不要更改csv输出目录
`./chBenchmark -csv -wh 10 -pa /var/lib/mysql-files  `  

### 运行数据库初始化，创建schema导入数据  
`./chBenchmark -init -dsn mysql-bench -usr root -pwd '123!@#200' -a 1 -t 0 -wd 30 -td 100 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  

./chBenchmark -mode generate -wh 10 -pa /var/lib/mysql-files

./chBenchmark -mode import -init -dsn mysql-bench -usr root -pwd '123!@#200' -a 0 -t 0 -wd 1 -td 1 -pa /var/lib/mysql-files -op /var/lib/mysql-files


# 只导入数据到数据库
./chBenchmark -mode import -dsn MyDB -usr user -pwd password -pa ./data

# 只运行基准测试
./chBenchmark -mode benchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 0 -t 1 -wd 10 -td 30 -pa /var/lib/mysql-files -op /var/lib/mysql-files
### 负载生成压缩  
拷贝查看  
docker cp d5aae99505cd:LLM-for-DB-Tuning/prompt/final_prompt.md ./final_prompt.md
### 运行ch-benchmark测试
-a是OLAP线程数量，-t是TP线程数，a=1,t=0时仅顺序执行22个查询测试AP_latency，a=0,t=1时仅顺序执行5个事务测试TP_latency。  
-wd是warmup duration，-td是test duration，限定了并发执行的时间，当测试latency时不受test duration控制，执行完毕后直接结束线程不会等待。  
一个测试AP_latency的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 0 -t 1 -wd 30 -td 30 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  
查看结果  
`cat /var/lib/mysql-files/latency_AP.txt  `  
一个测试TP_latency的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 0 -t 1 -wd 10 -td 30 -pa /var/lib/mysql-files -op /var/lib/mysql-files`  
查看结果  
`cat /var/lib/mysql-files/latency_TP.txt`  
一个测试tps等的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 5 -t 10 -wd 60 -td 300 -pa /var/lib/mysql-files -op /var/lib/mysql-files`  
查看结果  
`cat /var/lib/mysql-files/Result.txt`  


## PART2 归因权重（Attribution Weights）

PART2 会通过 EXPLAIN ANALYZE 把算子时间归因到列级操作（scan/order by/group by/join(col) 等）。为尽量“取到信息”，在无法精确识别时会采用分层回退分摊。为控制“误分摊”，引入了可调的权重参数：

- 环境变量与默认值（数值越小，分摊越谨慎）：
  - `PART2_W_JOIN_EQ=1.0` 等值连接列对准确匹配（a.b=c.d）的归因权重。
  - `PART2_W_JOIN_GENERIC=0.6` 无法解析出列对时，对 join 节点的通用分摊权重。
  - `PART2_W_FILTER_COLUMN=0.6` Filter 节点基于出现的列（alias.col）分摊到 scan 的权重。
  - `PART2_W_FILTER_GLOBAL=0.3` Filter 节点无法定位列/表时，对相关表 scan 的兜底分摊权重。
  - `PART2_W_SCAN_GENERIC=1.0` 表级扫描/索引扫描匹配后的分摊权重。

- 语义与优先级：
  1) JOIN：优先用“等值列对”精确归因；若缺失列对，仅在算子文本中提及的相关表上做通用分摊，并乘以 `PART2_W_JOIN_GENERIC`。
  2) FILTER：先尝试把时间分配给谓词中出现的列的 `scan`（乘 `PART2_W_FILTER_COLUMN`）；否则仅在相关表上做兜底分配（乘 `PART2_W_FILTER_GLOBAL`）。
  3) SCAN：扩展识别 `index lookup / (index) range scan / full table scan / table scan / lookup`，匹配到后在该表的 `scan` 目标上分配（乘 `PART2_W_SCAN_GENERIC`）。

- 调参建议：
  - 追求保守（减少误分摊）：降低 `PART2_W_JOIN_GENERIC` 与 `PART2_W_FILTER_GLOBAL`（如 0.4 / 0.0）。
  - 追求覆盖（尽量不丢时间）：适度提高上述权重，但建议保留相对次于精确匹配的比例关系。

- 示例：
  ```bash
  # 更保守：减少通用分摊
  export PART2_W_JOIN_GENERIC=0.4
  export PART2_W_FILTER_GLOBAL=0.0

  # 恢复默认
  unset PART2_W_JOIN_GENERIC PART2_W_FILTER_GLOBAL
  ```

- 调试：
  - `PART2_DEBUG=1`、`PART2_DEBUG_DIR=...` 可输出 `nodes/`（解析到的 EXPLAIN 节点）与 `per_key/`（列级时间归因），便于确认权重与分摊效果。
## Auto Loop（并行评估与优化）

本仓库的 `auto_loop.py` 提供并行多对话（m）、多轮推进（n）、统一评估选优（k）与入围优化（s）的自动流程，并在执行前提供人工确认门。核心能力与运行方式见下文。

### 性能评估（static / dynamic）

- static（默认）：
  - 基线来源：`prompt/PART2.py` 通过 MySQL EXPLAIN ANALYZE 对工作负载进行归因，得到每张表各列操作的 avg_time、count 以及总时间 `sum_time_ms`（毫秒）。
  - 改写模拟：用 `scripts/storage_transformer.StorageModel` 应用候选操作序列到存储元数据（meta.json），得到每表“行数 fr”与“行宽 fw=∑avg_length×(1-null_frac)”变化。
  - 重加权规则（本实现）：
    - 扫描：`cost_scan' = sum_time_ms_scan × fw × fr`
    - 连接：`cost_join' = sum_time_ms_join × mean(fw×fr, 邻表平均(fw×fr))`
    - 排序/聚合：`cost' = sum_time_ms × fw × fr × log2(N’)/log2(N)`（N≈表行数）
  - 说明：评估直接使用 PART2 的 `sum_time_ms`（EXPLAIN 归因总时间），不再用 `avg_time×count` 近似。

- dynamic：为每个候选创建临时库，导入 schema.sql，使用 runner 应用 schema+SQL 改写并导出改写后的 SQL，随后用 `query_latency/collect_latency.py` 采集真实延迟之和作为得分。

### 不落盘的 SQL 差异分析（用于评估参考）

当仅需比较“改写前后 SQL 所使用的表/连接对差异”以辅助评估，而不希望真正落盘改写时，可按以下思路实现轻量差异分析：

1) 解析候选操作序列（`response/runner.py` 中已有解析器可复用）。
2) 针对每条原始 SQL：
   - 使用 `sqlglot` 或 PART2 中的别名/列归属推断，提取 `FROM/JOIN` 的表集合与连接对。
   - 根据候选操作对“表名映射关系”进行替换（例如 `TableJoin(A,B)->NewAB` 视为把 (A,B) 合并为 NewAB）。
   - 输出改写前后的“表集合与连接对差异”，作为成本外推的补充特征，无需真正改写 SQL 文件。

建议后续提供 `analysis/sql_usage_diff.py` 脚本以自动化上述流程。

