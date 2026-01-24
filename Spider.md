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
`cp -r ./output_2026-01-24_01-13-09/* /var/lib/mysql-files/output_dir`
 
`mysql -u root -p tpcch`
mysql -u root -p'123!@#200' customer_deliveries 
`CREATE DATABASE customer_deliveries;`  
`USE customer_deliveries;`  
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
  python3 query_latency/collect_latency.py --sql-dir output_dir/sql --all-sql --frequencies output_dir/low_frequencies.csv --total-runs 200 --config query_latency/db_config.ini --output query_latency/low_latency_weighted.csv --error-output query_latency/latency_errors.csv
```   
查看结果  
cat query_latency/high_latency_weighted.csv

### 构造提示词  
```python  
EXEC_TOTAL_RUNS=200 PART2_DEBUG=1 PART2_DEBUG_DIR=LLM-for-DB-Tuning/part2_debug EXEC_COUNTS=LLM-for-DB-Tuning/output_dir/sql/high_frequencies.csv python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/output_dir/schema/schema.sql --csv-dir LLM-for-DB-Tuning/dbdatagen/output_2026-01-20_03-13-05 --sql-dir LLM-for-DB-Tuning/output_dir/sql --out LLM-for-DB-Tuning/prompt/final_prompt.md
```
拷贝查看  
`docker cp 514ec32a36d8:LLM-for-DB-Tuning/prompt/final_prompt.md ./spider_prompt.md`  

### 使用提示词与LLM对话 得到操作序列  
提示词路径
`cat prompt/final_prompt.md`


### 解析操作序列，调用改写功能 
先在response/response.txt写下序列  

逐个执行  
python3 LLM-for-DB-Tuning/test1.py --sql-dir LLM-for-DB-Tuning/response/rewritten_sql --out-dir LLM-for-DB-Tuning/response/rewritten1_sql --apply-schema --db-host localhost --db-port 3306 --db-user root --db-password '123!@#200' --db-name customer_deliveries
第二个之前UPDATE customers SET date_became_customer = NULL WHERE date_became_customer + 0 = 0;
第三个之前 UPDATE actual_orders SET actual_order_date = NULL WHERE actual_order_date + 0 = 0;
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


