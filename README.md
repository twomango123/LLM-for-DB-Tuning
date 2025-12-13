# rewrite接口 测试运行方法  
在当前目录创建并激活虚拟环境  
`python -m venv venv`
`source venv/bin/activate`

进入rewrite目录  
`cd rewrite`
可运行测试  
`python TableMerge.py`
部分已设置用例测试

# L0:evaluation-chbenchmark
### 创建镜像 运行并进入docker  
在git clone之前需要先创建一个工作目录，在工作目录下clone  
```bash
chmod +x docker.sh  
./docker.sh  
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


