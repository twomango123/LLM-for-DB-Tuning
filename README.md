# rewrite接口 测试运行方法  
在当前目录创建并激活虚拟环境  
`python -m venv venv`  
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
### 将数据集导入数据库  
`mysql -u root -p tpcch`  
`CREATE DATABASE csu_1;`  
`USE csu_1;` 
`SOURCE /LLM-for-DB-Tuning/scripts/schema.sql;`  

| Campuses  | 类型   |
| -------- | ---- |
| Id       | INT  |
| Campus   | TEXT |
| Location | TEXT |
| County   | TEXT |
| Year     | INT  |

| csu_fees   | 类型  |
| --------- | --- |
| Campus    | INT |
| Year      | INT |
| CampusFee | INT |

| degrees     | 类型  |
| ------- | --- |
| Year    | INT |
| Campus  | INT |
| Degrees | INT |

| discipline_enrollments  | 类型  |
| ------------- | --- |
| Campus        | INT |
| Discipline    | INT |
| Year          | INT |
| Undergraduate | INT |
| Graduate      | INT |

| enrollments            | 类型  |
| ------------------ | --- |
| Campus             | INT |
| Year               | INT |
| TotalEnrollment_AY | INT |
| FTE_AY             | INT |

| faculty     | 类型     |
| ------- | ------ |
| Campus  | INT    |
| Year    | INT    |
| Faculty | DOUBLE |




### 测试历史查询延迟  
```python
python3 query_latency/collect_latency.py \
  --sql-dir spider_data/spider_data/database_mysql/csu_1 \
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
`python3 ./scripts/build_prompt.py ./spider_data/spider_data/database_mysql/csu_1 --prompt prompt.md`  

### 使用提示词与LLM对话 得到操作序列  
第一次：  
TableJoin(Campuses, csu_fees, Campuses.Id, csu_fees.Campus, False):Campuses_csu_fees

TableJoin(Campuses_csu_fees, degrees, Campuses_csu_fees.Id, degrees.Campus, False):Campuses_degrees  

TableJoin(Campuses_degrees, faculty, Campuses_degrees.Id, faculty.Campus, False):Campuses_faculty  

HorizontalSplit(Campuses_faculty):Campuses_faculty_losangeles(county="Los Angeles"),Campuses_faculty_others(county<>"Los Angeles")  

RedundantColumnAdd(Campuses_faculty_losangeles.Campus, degrees)

RedundantColumnAdd(Campuses_faculty_losangeles.Campus, enrollments)    





### 解析操作序列，调用改写功能  


### 测试改写后查询延迟   
```python3 query_latency/collect_latency.py \
  --sql-dir query_latency/new_latency \
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
### 运行生成数据命令 可指定warehouse数量（-wh 1） 最好不要更改csv输出目录
`./chBenchmark -csv -wh 1 -pa /var/lib/mysql-files  `  

### 运行数据库初始化，创建schema导入数据  
`./chBenchmark -init -dsn mysql-bench -usr root -pwd '123!@#200' -a 1 -t 0 -wd 30 -td 100 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  

### 运行ch-benchmark测试
-a是OLAP线程数量，-t是TP线程数，a=1,t=0时仅顺序执行22个查询测试AP_latency，a=0,t=1时仅顺序执行5个事务测试TP_latency。  
-wd是warmup duration，-td是test duration，限定了并发执行的时间，当测试latency时不受test duration控制，执行完毕后直接结束线程不会等待。  
一个测试AP_latency的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 1 -t 0 -wd 30 -td 100 -pa /var/lib/mysql-files -op /var/lib/mysql-files `  
查看结果  
`cat /var/lib/mysql-files/latency_AP.txt  `  
一个测试TP_latency的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 0 -t 1 -wd 30 -td 100 -pa /var/lib/mysql-files -op /var/lib/mysql-files`  
查看结果  
`cat /var/lib/mysql-files/latency_TP.txt`  
一个测试tps等的示例  
`./chBenchmark -run -dsn mysql-bench -usr root -pwd '123!@#200' -a 5 -t 10 -wd 60 -td 300 -pa /var/lib/mysql-files -op /var/lib/mysql-files`  
查看结果  
`cat /var/lib/mysql-files/Result.txt`  


