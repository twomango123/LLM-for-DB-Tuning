

# 初始化chbenchmark-进行基准测试  
### 创建镜像 运行并进入docker  
在git clone之前需要先创建一个工作目录，在工作目录下clone  
```bash
chmod +x docker.sh  
./docker.sh  
```   
### 进入docker后 启动mysql服务  
`service mysql start`  
 
### 进入ch-benchmark 编译
```bash
cd ch-benchmark  
make clean  
make  
```  

### 运行生成数据命令 可指定warehouse数量（-wh 1） 最好不要更改csv输出目录
`./chBenchmark -csv -wh 5 -pa /var/lib/mysql-files  `  

### 运行数据库初始化，创建schema导入数据  
`./chBenchmark -init -dsn mysql-bench -usr root -pwd '123!@#200' -a 1 -t 0 -wd 30 -td 100 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  


### 运行ch-benchmark测试
-a是OLAP线程数量，-t是TP线程数，a=1,t=0时仅顺序执行22个查询测试AP_latency，a=0,t=1时仅顺序执行5个事务测试TP_latency。  
-wd是warmup duration，-td是test duration，限定了并发执行的时间，当测试latency时不受test duration控制，执行完毕后直接结束线程不会等待。  

一个测试AP_latency的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 1 -t 0 -wd 30 -td 30 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  
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


### 负载生成压缩  

### 构造提示词
PART2_DEBUG=1 PART2_DEBUG_DIR=LLM-for-DB-Tuning/part2_debug EXEC_COUNTS=LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update/sample_execution_counts_chbench.csv DB_CONFIG=LLM-for-DB-Tuning/query_latency/db_config.ini python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/DataBase/cleaned_sql/schema.sql --csv-dir /var/lib/mysql-files --sql-dir LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update --out LLM-for-DB-Tuning/prompt/final_prompt.md

PART2_DEBUG=1 EXEC_COUNTS=LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update/sample_execution_counts_chbench.csv DB_CONFIG=LLM-for-DB-Tuning/query_latency/db_config.ini python3 LLM-for-DB-Tuning/prompt/COMBINATION.py --schema-sql LLM-for-DB-Tuning/DataBase/cleaned_sql/schema.sql --csv-dir /var/lib/mysql-files --sql-dir LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update --out LLM-for-DB-Tuning/prompt/final_prompt.md

86e5fc9c7950
cat prompt/final_prompt.md

拷贝查看  
`docker cp ee7402c6a770:LLM-for-DB-Tuning/prompt/final_prompt.md ./ch_prompt.md`  

### 操作序列转自动化改写  
备份原SQL    
`cp ../DataBase/cleaned_sql/schema.sql /var/lib/mysql-files/schema.sql`  
`cp /DataBase/cleaned_sql/query_and_update /var/lib/mysql-files/clean_sql_copy`


检查数据库内情况  
`mysql -u root -p tpcch`
### 解析操作序列，调用改写功能  

python3 LLM-for-DB-Tuning/response/runner.py --use-db --host localhost --port 3306 --user root --password '123!@#200' --database tpcch --sql-dir LLM-for-DB-Tuning/Data/cleaned_sql/query_and_update --out-sql-dir LLM-for-DB-Tuning/response/rewritten_sql

python3 LLM-for-DB-Tuning/test2.py --sql-dir LLM-for-DB-Tuning/response/rewritten1_sql --out-dir LLM-for-DB-Tuning/response/rewritten2_sql --apply-schema --db-host localhost --db-port 3306 --db-user root --db-password '123!@#200' --db-name tpcch






