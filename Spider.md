### 创建镜像 运行并进入docker  
在git clone之前需要先创建一个工作目录，在工作目录下clone  
```bash
chmod +x docker.sh  
./docker.sh  
```   
### 进入docker后 启动mysql服务  
`service mysql start`  

### 生成扩充数据  
cd DBDataGen
python3 generator.py
### 将数据集导入数据库  
cd /var/lib/mysql-files 
mkdir output_dir
cd /LLM-for-DB-Tuning/DBDataGen
`cp -r ./output_2026-01-31_07-42-33/* /var/lib/mysql-files/output_dir`
 
`mysql -u root -p tpcch`
mysql -u root -p'123!@#200' customer_deliveries 
DROP DATABASE customer_deliveries;
CREATE DATABASE customer_deliveries; 
USE customer_deliveries;
### 回滚数据库  
mysql -u root -p'123!@#200' --default-character-set=utf8mb4 -D customer_deliveries < output_dir/schema/rollback.sql
### 备份初始数据库  

mysqldump -uroot -p customer_deliveries > backup.sql  

CREATE DATABASE restore_db;
mysql -uroot -p restore_db < base_backup.sql

建表执行schema/schema.sql  
导入csv执行schema/insert_sql.sql  
`cp -r output_dir/rewritten /var/lib/mysql-files/output_dir/query`


### 准备历史查询（一般忽略）
python3 spider_data/spider_data/export_train_queries.py --input spider_data/spider_data/train_spider.json --db-root spider_data/spider_data/database_mysql  

### 测试历史查询延迟  
修改数据库名称  
`nano query_latency/db_config.ini`    
执行测试  
```python
python3 query_latency/collect_latency.py \
  --sql-dir output_dir/sql --all-sql \
  --config query_latency/db_config.ini \
  --output query_latency/latency_results.csv \
  --error-output query_latency/latency_errors.csv

200次SQL读写延迟统计 分别测试high-mid-low写读比  
  python3 query_latency/collect_latency.py --sql-dir output_dir/sql --all-sql --frequencies output_dir/sql/low_frequencies.csv --total-runs 200 --config query_latency/db_config.ini --output query_latency/low_latency_weighted.csv --error-output query_latency/latency_errors.csv
```   
改写后
python3 query_latency/collect_latency.py --sql-dir response/rewritten_sql_full --all-sql --frequencies output_dir/sql/low_frequencies.csv --total-runs 200 --config query_latency/db_config.ini --output query_latency/low_latency_weighted_rewritten.csv --error-output query_latency/latency_errors_rewritten.csv
查看结果  
cat query_latency/high_latency_weighted.csv

### 构造提示词  
```python  
EXEC_TOTAL_RUNS=200 PART2_DEBUG=1 PART2_DEBUG_DIR=LLM-for-DB-Tuning/part2_debug EXEC_COUNTS=LLM-for-DB-Tuning/output_dir/sql/high_frequencies.csv python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/output_dir/schema/schema.sql --csv-dir LLM-for-DB-Tuning/dbdatagen/output_2026-01-31_07-42-33 --sql-dir LLM-for-DB-Tuning/output_dir/sql --out LLM-for-DB-Tuning/prompt/prompt.md
```
拷贝查看  
`docker cp 514ec32a36d8:LLM-for-DB-Tuning/prompt/final_prompt.md ./spider_prompt.md`  

### 使用提示词与LLM对话 得到操作序列  
提示词路径
`cat prompt/final_prompt.md`


### 解析操作序列，调用改写功能  
不连库 全自动校验  
python3 auto_loop.py --max-iters 5 --schema-sql output_dir/schema/schema.sql --sql-dir output_dir/sql --out-sql-dir response/rewritten_sql --storage-meta output_dir/meta.json --storage-budget 20GB  

连库 全自动进行6轮
需要先去0值 mysql -u root -p'123!@#200' customer_deliveries 
UPDATE customers SET date_became_customer = NULL WHERE date_became_customer + 0 = 0;
UPDATE actual_orders SET actual_order_date = NULL WHERE actual_order_date + 0 = 0;
UPDATE order_deliveries SET delivery_date = NULL WHERE delivery_date + 0 = 0;
UPDATE customer_addresses SET date_from = '1970-01-01 00:00:00' WHERE date_from + 0 = 0;
UPDATE customer_addresses SET date_to = NULL WHERE date_to + 0 = 0;  
更新插入程序计时  
python3 query_latency/dml_timer.py --sql-dir output_dir/sql --host localhost --port 3306 --user root --password '123!@#200' --database customer_deliveries --out debug/part2/dml_time_cache.json
提示词生成  
DML_TIME_CACHE=LLM-for-DB-Tuning/debug/part2/dml_time_cache.json EXEC_TOTAL_RUNS=200 PART2_DEBUG=1 PART2_DEBUG_DIR=LLM-for-DB-Tuning/part2_debug EXEC_COUNTS=LLM-for-DB-Tuning/output_dir/sql/high_frequencies.csv python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/output_dir/schema/schema.sql --csv-dir LLM-for-DB-Tuning/dbdatagen/output_2026-01-31_07-42-33 --sql-dir LLM-for-DB-Tuning/output_dir/sql --out LLM-for-DB-Tuning/prompt/prompt.md

存储开销初始生成
python3 scripts/generate_meta_mysql.py --schema-sql output_dir/schema/schema.sql --host localhost --port 3306 --user root --password '123!@#200' --database customer_deliveries --out output_dir/meta.json
全自动进行6轮
python3 auto_loop.py --max-iters 5 --use-db --host localhost --port 3306 --user root --password '123!@#200' --database customer_deliveries --sql-dir output_dir/sql --out-sql-dir response/rewritten_sql --schema-sql output_dir/schema/schema.sql --storage-meta output_dir/meta.json --storage-budget 10GB

静态运行
python3 auto_loop.py --parallel-m 4 --rounds-n 5 --select-k 1 --opt-rounds-s 2 --schema-sql output_dir/schema/schema.sql --eval-db-config query_latency/db_config.ini --eval-sql-dir output_dir/sql --storage-meta output_dir/meta.json --eval-mode --eval-mode both
动态运行
python3 auto_loop.py --parallel-m 4 --rounds-n 5 --select-k 1 --opt-rounds-s 2 --schema-sql output_dir/schema/schema.sql --eval-sql-dir output_dir/sql --eval-mode static --no-db-eval --use-explain-debug --explain-debug-dir part2_debug
落库执行（询问后）
python3 auto_loop.py --parallel-m 4 --rounds-n 5 --select-k 1 --opt-rounds-s 2 --schema-sql output_dir/schema/schema.sql --eval-sql-dir output_dir/sql --eval-mode static --no-db-eval --use-explain-debug --explain-debug-dir part2_debug --eval-db-config query_latency/db_config.ini --storage-meta output_dir/meta.json --storage-budget 10GB

python3 auto_loop.py --parallel-m 4 --rounds-n 5 --select-k 2 --opt-rounds-s 20 --schema-sql output_dir/schema/schema.sql --eval-sql-dir output_dir/sql --sql-dir output_dir/sql --eval-mode static --no-db-eval --use-explain-debug --explain-debug-dir part2_debug --eval-db-config query_latency/db_config.ini --storage-meta output_dir/meta.json --storage-budget 10GB --stats-dir stats/spider

python3 auto_loop.py --parallel-m 4 --rounds-n 5 --select-k 1 --opt-rounds-s 2 --schema-sql DataBase/cleaned_sql/schema.sql --eval-db-config query_latency/db_config.ini --eval-sql-dir Data/cleaned_sql/query_and_update --storage-meta output_dir/meta.json --eval-mode static
逐个执行  

python3 LLM-for-DB-Tuning/test1.py --sql-dir LLM-for-DB-Tuning/response/rewritten_sql --out-dir LLM-for-DB-Tuning/response/rewritten1_sql --apply-schema --db-host localhost --db-port 3306 --db-user root --db-password '123!@#200' --db-name customer_deliveries

UPDATE customers SET date_became_customer = NULL WHERE date_became_customer + 0 = 0;
UPDATE actual_orders SET actual_order_date = NULL WHERE actual_order_date + 0 = 0;
UPDATE order_deliveries SET delivery_date = NULL WHERE delivery_date + 0 = 0;
UPDATE customer_addresses SET date_from = '1970-01-01 00:00:00' WHERE date_from + 0 = 0;


UPDATE customer_addresses SET date_to = NULL WHERE date_to + 0 = 0;

DROP TABLE IF EXISTS customer_with_address;

python3 LLM-for-DB-Tuning/test.py --sql-dir LLM-for-DB-Tuning/output_dir/sql --out-dir LLM-for-DB-Tuning/response/rewritten1_sql --apply-schema --db-host localhost --db-port 3306 --db-user root --db-password '123!@#200' --db-name customer_deliveries

```python
python3 LLM-for-DB-Tuning/response/runner.py --use-db --host localhost --port 3306 --user root --password '123!@#200' --database customer_deliveries --sql-dir LLM-for-DB-Tuning/output_dir/sql --out-sql-dir LLM-for-DB-Tuning/response/rewritten_sql
```  
mysql -u root -p customer_deliveries

### 测试改写后查询延迟   
```python3 query_latency/collect_latency.py \
  --sql-dir output_dir/rewritten \
  --config query_latency/db_config.ini \
  --output query_latency/new_results.csv \
  --error-output query_latency/new_errors.csv
  ```


