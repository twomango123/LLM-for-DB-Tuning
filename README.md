# L0:evaluation
### 环境准备
python -m venv .venv  
.venv\Scripts\activate      # Windows  
pip install -r requirements.txt   
### 运行 包含mysql database删除和创建，schema创建，源数据导入，sql语句导入并执行查询收集指标（预热，串行，并行，latency和吞吐量）
python ./DataBase/MySQLDriver.py  
### 测试结果  
cd ./DataBase/tpcc_benchmark_results.json  

# 其他前置工作
### ch-benchmark的tpc-h queries，来自于ch-benchmark-mysqlDialect
cd DataBase/cleaned_sql  
### 源数据
cd ./tpcc_data  
### 源数据生成 来自ch-benchmark中生成数据的c++程序改写
cd Data/DataSource/DataGen  
g++ -o tpcc_data_gen main.cpp DataSource.cpp TupleGen.cpp -std=c++11 -lpthread  
./tpcc_data_gen -w 1 -o D:\LLM4DBTUNING\tpcc_data 
