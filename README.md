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
`./chBenchmark -mode generate -csv -wh 10 -pa /var/lib/mysql-files  `  

### 运行数据库初始化，创建schema导入数据  
`./chBenchmark -init -dsn mysql-bench -usr root -pwd '123!@#200' -a 1 -t 0 -wd 30 -td 100 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  

./chBenchmark -mode generate -wh 10 -pa /var/lib/mysql-files

./chBenchmark -mode import -init -dsn mysql-bench -usr root -pwd '123!@#200' -a 0 -t 0 -wd 1 -td 1 -pa /var/lib/mysql-files -op /var/lib/mysql-files


# 只导入数据到数据库
./chBenchmark -mode import -dsn MyDB -usr user -pwd password -pa ./data

# 只运行基准测试
./chBenchmark -mode benchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 1 -t 0 -wd 10 -td 30 -pa /var/lib/mysql-files -op /var/lib/mysql-files
### 负载生成压缩  
拷贝查看  
docker cp d5aae99505cd:LLM-for-DB-Tuning/prompt/final_prompt.md ./final_prompt.md
### 运行ch-benchmark测试
-a是OLAP线程数量，-t是TP线程数，a=1,t=0时仅顺序执行22个查询测试AP_latency，a=0,t=1时仅顺序执行5个事务测试TP_latency。  
-wd是warmup duration，-td是test duration，限定了并发执行的时间，当测试latency时不受test duration控制，执行完毕后直接结束线程不会等待。  
一个测试AP_latency的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 3 -t 0 -wd 30 -td 30 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  
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


