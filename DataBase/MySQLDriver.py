import mysql.connector
from mysql.connector import errorcode
from DatabaseDriver import DatabaseDriver
from typing import List, Dict, Any, Tuple
import threading
import os
import time
from queue import Queue
import json

class MySQLDriver(DatabaseDriver):
    """MySQL 方言实现 - 完整 TPC-C schema 支持"""

    def connect(self) -> bool:
        try:
            self.connection = mysql.connector.connect(
                host=self.config.get("host", "localhost"),
                port=self.config.get("port", 3306),
                user=self.config["user"],
                password=self.config["password"],
                database=self.config.get("database"),
                allow_local_infile=True
            )
            self.is_connected = True
            return True
        except mysql.connector.Error as err:
            print(f"连接数据库失败: {err}")
            self.is_connected = False
            return False

    def disconnect(self) -> bool:
        if self.connection:
            self.connection.close()
            self.is_connected = False
        return True

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        if not self.is_connected:
            raise RuntimeError("数据库未连接")
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def execute_statement(self, statement: str) -> bool:
        if not self.is_connected:
            raise RuntimeError("数据库未连接")
        try:
            cursor = self.connection.cursor()
            cursor.execute(statement)
            self.connection.commit()
            cursor.close()
            return True
        except mysql.connector.Error as err:
            print(f"执行语句失败: {err}\nSQL: {statement}")
            return False

    def drop_schema(self) -> bool:
        dbname = self.config.get("database", "tpcch")
        return self.execute_statement(f"DROP DATABASE IF EXISTS {dbname}")

    def create_schema(self) -> bool:
        """完整创建 TPC-C schema"""
        dbname = self.config.get("database", "tpcch")
        statements = [
            "CREATE DATABASE tpcch",
            """CREATE TABLE tpcch.warehouse (
                w_id integer,
                w_name char(10),
                w_street_1 char(20),
                w_street_2 char(20),
                w_city char(20),
                w_state char(2),
                w_zip char(9),
                w_tax decimal(4,4),
                w_ytd decimal(12,2),
                PRIMARY KEY (w_id)
            )""",
            """CREATE TABLE tpcch.district (
                d_id tinyint,
                d_w_id integer,
                d_name char(10),
                d_street_1 char(20),
                d_street_2 char(20),
                d_city char(20),
                d_state char(2),
                d_zip char(9),
                d_tax decimal(4,4),
                d_ytd decimal(12,2),
                d_next_o_id integer,
                PRIMARY KEY (d_w_id, d_id)
            )""",
            "CREATE INDEX fk_district_warehouse ON tpcch.district (d_w_id ASC)",
            """CREATE TABLE tpcch.customer (
                c_id smallint,
                c_d_id tinyint,
                c_w_id integer,
                c_first char(16),
                c_middle char(2),
                c_last char(16),
                c_street_1 char(20),
                c_street_2 char(20),
                c_city char(20),
                c_state char(2),
                c_zip char(9),
                c_phone char(16),
                c_since DATE,
                c_credit char(2),
                c_credit_lim decimal(12,2),
                c_discount decimal(4,4),
                c_balance decimal(12,2),
                c_ytd_payment decimal(12,2),
                c_payment_cnt smallint,
                c_delivery_cnt smallint,
                c_data text,
                c_n_nationkey integer,
                PRIMARY KEY(c_w_id, c_d_id, c_id)
            )""",
            "CREATE INDEX fk_customer_district ON tpcch.customer (c_w_id ASC, c_d_id ASC)",
            """CREATE TABLE tpcch.history (
                h_c_id smallint,
                h_c_d_id tinyint,
                h_c_w_id integer,
                h_d_id tinyint,
                h_w_id integer,
                h_date date,
                h_amount decimal(6,2),
                h_data char(24)
            )""",
            "CREATE INDEX fk_history_customer ON tpcch.history (h_c_w_id ASC, h_c_d_id ASC, h_c_id ASC)",
            "CREATE INDEX fk_history_district ON tpcch.history (h_w_id ASC, h_d_id ASC)",
            """CREATE TABLE tpcch.neworder (
                no_o_id integer,
                no_d_id tinyint,
                no_w_id integer,
                PRIMARY KEY (no_w_id, no_d_id, no_o_id)
            )""",
            """CREATE TABLE tpcch.order (
                o_id integer,
                o_d_id tinyint,
                o_w_id integer,
                o_c_id smallint,
                o_entry_d date,
                o_carrier_id tinyint,
                o_ol_cnt tinyint,
                o_all_local tinyint,
                PRIMARY KEY (o_w_id, o_d_id, o_id)
            )""",
            "CREATE INDEX fk_order_customer ON tpcch.order (o_w_id ASC, o_d_id ASC, o_c_id ASC)",
            """CREATE TABLE tpcch.orderline (
                ol_o_id integer,
                ol_d_id tinyint,
                ol_w_id integer,
                ol_number tinyint,
                ol_i_id integer,
                ol_supply_w_id integer,
                ol_delivery_d date,
                ol_quantity smallint,
                ol_amount decimal(6,2),
                ol_dist_info char(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            )""",
            "CREATE INDEX fk_orderline_order ON tpcch.orderline (ol_w_id ASC, ol_d_id ASC, ol_o_id ASC)",
            "CREATE INDEX fk_orderline_stock ON tpcch.orderline (ol_supply_w_id ASC, ol_i_id ASC)",
            """CREATE TABLE tpcch.item (
                i_id integer,
                i_im_id smallint,
                i_name char(24),
                i_price decimal(5,2),
                i_data char(50),
                PRIMARY KEY (i_id)
            )""",
            """CREATE TABLE tpcch.stock (
                s_i_id integer,
                s_w_id integer,
                s_quantity integer,
                s_dist_01 char(24),
                s_dist_02 char(24),
                s_dist_03 char(24),
                s_dist_04 char(24),
                s_dist_05 char(24),
                s_dist_06 char(24),
                s_dist_07 char(24),
                s_dist_08 char(24),
                s_dist_09 char(24),
                s_dist_10 char(24),
                s_ytd integer,
                s_order_cnt integer,
                s_remote_cnt integer,
                s_data char(50),
                s_su_suppkey integer,
                PRIMARY KEY (s_w_id, s_i_id)
            )""",
            "CREATE INDEX fk_stock_warehouse ON tpcch.stock (s_w_id ASC)",
            "CREATE INDEX fk_stock_item ON tpcch.stock (s_i_id ASC)",
            """CREATE TABLE tpcch.nation (
                n_nationkey tinyint NOT NULL,
                n_name char(25) NOT NULL,
                n_regionkey tinyint NOT NULL,
                n_comment char(152) NOT NULL,
                PRIMARY KEY (n_nationkey)
            )""",
            """CREATE TABLE tpcch.supplier (
                su_suppkey smallint NOT NULL,
                su_name char(25) NOT NULL,
                su_address char(40) NOT NULL,
                su_nationkey tinyint NOT NULL,
                su_phone char(15) NOT NULL,
                su_acctbal decimal(12,2) NOT NULL,
                su_comment char(101) NOT NULL,
                PRIMARY KEY (su_suppkey)
            )""",
            """CREATE TABLE tpcch.region (
                r_regionkey tinyint NOT NULL,
                r_name char(55) NOT NULL,
                r_comment char(152) NOT NULL,
                PRIMARY KEY (r_regionkey)
            )"""
        ]
        success = True
        for stmt in statements:
            if not self.execute_statement(stmt):
                success = False
        return success

    def import_csv(self, table_name: str, csv_file: str, delimiter: str = '|') -> bool:
        """使用 LOAD DATA INFILE 导入 CSV，提高大规模数据加载速度"""
        if not os.path.exists(csv_file):
            print(f"CSV 文件不存在: {csv_file}")
            return False
        try:
            cursor = self.connection.cursor()
            sql = (
                f"LOAD DATA LOCAL INFILE '{csv_file}' "
                f"INTO TABLE `{table_name}` "
                f"FIELDS TERMINATED BY '{delimiter}' "
                f"LINES TERMINATED BY '\n' "
                # f"IGNORE 1 LINES"
            )
            cursor.execute(sql)
            self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            print(f"LOAD DATA INFILE 导入失败: {e}")
            return False

    def check_data_integrity(self, warehouse_count=1) -> bool:
        """检查 TPC-C/H 数据完整性"""
        tables = {
            "warehouse": 1*warehouse_count,   # 可根据实际 warehouse 数量调整
            "district": 10*warehouse_count,   # 每 warehouse 10 个 district
            "customer": 30000*warehouse_count, # 每 district 3000 customer
            "order": 30000*warehouse_count,
            "orderline": 300000*warehouse_count,
            "neworder": 9000*warehouse_count,
            "history": 30000*warehouse_count,
            "stock": 100000*warehouse_count,
            "item": 100000*warehouse_count,
            "supplier": 10000*warehouse_count,
            "nation": 62*warehouse_count,
            "region": 5*warehouse_count,
            
        }
        dbname = self.config.get("database", "tpcch")
        for table, expected_count_per_warehouse in tables.items():
            query = f"SELECT COUNT(*) AS cnt FROM `{table}`"
            try:
                rows = self.execute_query(query)
                cnt = rows[0]['cnt']
                print(f"{table} 行数: {cnt}")
                if cnt == 0:
                    print(f"表 {table} 数据不完整")
                    return False
            except Exception as e:
                print(f"检查 {table} 失败: {e}")
                return False
        return True
    

    def _create_thread_connection(self):
        """为每个线程创建独立的数据库连接"""
        return mysql.connector.connect(
            host=self.config.get("host", "localhost"),
            port=self.config.get("port", 3306),
            user=self.config["user"],
            password=self.config["password"],
            database=self.config.get("database"),   # 数据库名
            allow_local_infile=True                  # 支持 LOAD DATA LOCAL INFILE
        )

    def execute_with_timing(self, query: str) -> Tuple[List[Dict], float]:
        """执行查询并记录时间"""
        start_time = time.time()
        try:
            result = self.execute_query(query)
            execution_time = time.time() - start_time
            return result, execution_time
        except Exception as e:
            execution_time = time.time() - start_time
            raise e

    def _execute_concurrent_query(self, query: str, duration: float, results_queue: Queue):
        """并发执行查询的线程函数"""
        query_count = 0
        thread_times = []
        end_time = time.time() + duration
        
        thread_conn = self._create_thread_connection()
        
        while time.time() < end_time:
            start_time = time.time()
            try:
                if thread_conn:
                    cursor = thread_conn.cursor()
                    cursor.execute(query)
                    cursor.fetchall()
                    cursor.close()
                execution_time = time.time() - start_time
                query_count += 1
                thread_times.append(execution_time)
            except Exception:
                #记录失败，继续执行
                continue
        
        if thread_conn:
            thread_conn.close()
        
        results_queue.put({
            'query_count': query_count,
            'thread_times': thread_times
        })

    def _calculate_throughput_metrics(self, total_queries: int, total_duration: float, 
                                    all_times: List[float]) -> Dict[str, Any]:
        """计算吞吐量相关指标"""
        tps = total_queries / total_duration if total_duration > 0 else 0
        qps = tps  # 对于查询来说，TPS和QPS相同
        
        avg_latency = sum(all_times) / len(all_times) if all_times else 0
        min_latency = min(all_times) if all_times else 0
        max_latency = max(all_times) if all_times else 0
        
        throughput_efficiency = (tps / avg_latency) if avg_latency > 0 else 0
        
        return {
            'throughput_tps': tps,
            'throughput_qps': qps,
            'total_queries': total_queries,
            'total_duration': total_duration,
            'avg_latency': avg_latency,
            'min_latency': min_latency,
            'max_latency': max_latency,
            'throughput_efficiency': throughput_efficiency,
            'queries_per_second_per_thread': tps / len(all_times) if all_times else 0
        }

    def _run_concurrent_test(self, query: str, concurrency: int, duration: float) -> Dict[str, Any]:
        """运行并发测试 测吞吐量"""
        print(f"并发吞吐量测试 - 线程数: {concurrency}, 持续时间: {duration}秒")
        
        results_queue = Queue()
        threads = []
        all_times = []
        total_queries = 0
        
        # 记录测试开始时间
        start_time = time.time()
        
        # 启动所有并发线程
        for i in range(concurrency):
            thread = threading.Thread(
                target=self._execute_concurrent_query,
                args=(query, duration, results_queue)
            )
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 计算总测试时间
        total_duration = time.time() - start_time
        
        # 收集所有线程的结果
        while not results_queue.empty():
            result = results_queue.get()
            total_queries += result['query_count']
            all_times.extend(result['thread_times'])
        
        # 计算吞吐量指标
        throughput_metrics = self._calculate_throughput_metrics(total_queries, total_duration, all_times)
        
        # 构建完整结果
        results = {
            'test_mode': 'concurrent_throughput',
            'concurrency_level': concurrency,
            'target_duration': duration,
            'actual_duration': total_duration,
        }
        results.update(throughput_metrics)
        
        print(f"并发吞吐量测试完成 - TPS: {throughput_metrics['throughput_tps']:.2f}, "
            f"总查询数: {total_queries}")
        
        return results

    def _run_sequential_test(self, query: str, iterations: int) -> Dict[str, Any]:
        """运行串行测试 测延迟"""
        print(f"串行延迟测试 - 迭代次数: {iterations}")
        
        latencies = []
        successful_runs = 0
        
        start_time = time.time()
        
        for i in range(iterations):
            try:
                _, execution_time = self.execute_with_timing(query)
                latencies.append(execution_time)
                successful_runs += 1
                print(f"执行 {i+1}/{iterations}: {execution_time:.4f}s")
            except Exception as e:
                print(f"执行 {i+1}/{iterations}: 失败 - {e}")
        
        
        total_time = time.time() - start_time
        
        if successful_runs == 0:
            raise RuntimeError("所有SQL执行都失败了")
        
        #计算串行模式的吞吐量
        tps = successful_runs / total_time if total_time > 0 else 0
        avg_latency = sum(latencies) / len(latencies)
        
        results = {
            'test_mode': 'sequential_latency',
            'total_iterations': iterations,
            'successful_iterations': successful_runs,
            'total_time': total_time,
            'throughput_tps': tps,
            'throughput_qps': tps,
            'avg_latency': avg_latency,
            'min_latency': min(latencies),
            'max_latency': max(latencies),
            'latencies': latencies,
            'throughput_efficiency': (tps / avg_latency) if avg_latency > 0 else 0
        }
        
        print(f"串行测试完成 - 平均延迟: {avg_latency:.4f}s, TPS: {tps:.2f}")
        
        return results

    def _warmup_sql(self, sql: str, warmup_runs: int = 3):
        """预热执行SQL"""
        print("开始预热...")
        for i in range(warmup_runs):
            try:
                start_time = time.time()
                self.execute_query(sql)
                execution_time = time.time() - start_time
                print(f"预热执行 {i+1}/{warmup_runs}: {execution_time:.4f}s")
            except Exception as e:
                print(f"预热执行 {i+1}/{warmup_runs}: 失败 - {e}")

    def evaluation(self, data: str, physical_schema: str, benchmark_sql: str, 
                iterations: int = 10, concurrency: int = 1, 
                duration: float = None) -> Dict[str, Any]:
        """
        L0-evaluation：执行基准SQL并返回TPS和latency
        
        Args:
            data: 数据
            physical_schema: schema信息
            benchmark_sql: 测试SQL
            iterations: 执行迭代次数(串行)
            concurrency: 并发线程数
            duration: 并发测试持续时间s
            
        Returns:
            包含tps latency等指标的dict
        """
        print(f"开始性能评估")
        print(f"数据: {data}")
        print(f"Schema: {physical_schema}")
        print(f"测试SQL: {benchmark_sql}")
        
        #预热
        self._warmup_sql(benchmark_sql)
        
        if concurrency > 1 and duration:
            #并发吞吐量测试
            results = self._run_concurrent_test(benchmark_sql, concurrency, duration)
        else:
            #串行延迟测试
            results = self._run_sequential_test(benchmark_sql, iterations)
        
        # 添加公共信息
        results.update({
            'data': data,
            'physical_schema': physical_schema,
            'benchmark_sql': benchmark_sql,
        })
        
        # 输出主要结果
        tps_value = results.get('throughput_tps', 0)
        latency_value = results.get('avg_latency', 0)
        print(f"评估完成 - TPS: {tps_value:.2f}, avg_latency: {latency_value:.4f}s")
        
        return results





# def clean_and_save_sql(input_file, output_dir="cleaned_sql"):
#     """智能清洗SQL并保存到新文件"""
    
#     # 创建输出目录
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
#         print(f"创建输出目录: {output_dir}")
    
#     # 1. 读取原始文件
#     print(f"读取文件: {input_file}")
#     with open(input_file, 'r', encoding='utf-8') as f:
#         content = f.read()
    
#     # 2. 智能清洗SQL
#     sql_blocks = []
#     current_block = []
#     in_sql_block = False
#     query_number = 1
    
#     lines = content.split('\n')
    
#     for line in lines:
#         line = line.strip()
        
#         # 检测查询开始（TPC-H Query）
#         if line.startswith('// TPC-H-Query'):
#             if current_block and in_sql_block:
#                 # 完成前一个SQL块
#                 sql = ''.join(current_block)
#                 sql = sql.replace('\\n', '\n').replace('\\t', '\t').strip()
                
#                 # 移除末尾逗号并添加分号
#                 if sql.endswith(','):
#                     sql = sql[:-1].strip()
#                 if sql and not sql.endswith(';'):
#                     sql += ';'
                
#                 sql_blocks.append((query_number, sql))
#                 query_number += 1
#                 current_block = []
#             in_sql_block = True
#             continue
        
#         # 处理SQL字符串行
#         if in_sql_block and line.startswith('"'):
#             sql_line = line.strip('",')
#             sql_line = sql_line.replace('\\n', '\n').replace('\\t', '\t')
#             current_block.append(sql_line)
    
#     # 处理最后一个SQL块
#     if current_block and in_sql_block:
#         sql = ''.join(current_block)
#         sql = sql.replace('\\n', '\n').replace('\\t', '\t').strip()
#         if sql.endswith(','):
#             sql = sql[:-1].strip()
#         if sql and not sql.endswith(';'):
#             sql += ';'
#         sql_blocks.append((query_number, sql))
    
#     print(f"成功清洗出 {len(sql_blocks)} 个SQL查询")
    
#     # 3. 保存为不同格式的文件
    
    
    
#     # # 3.2 保存为可执行的SQL文件
#     # sql_file = os.path.join(output_dir, "benchmark_queries.sql")
#     # with open(sql_file, 'w', encoding='utf-8') as f:
#     #     f.write('-- ============================================\n')
#     #     f.write('-- TPC-H Benchmark Queries\n')
#     #     f.write('-- Automatically cleaned and formatted\n')
#     #     f.write('-- Generated from: ' + os.path.basename(input_file) + '\n')
#     #     f.write('-- ============================================\n\n')
        
#     #     for num, sql in sql_blocks:
#     #         f.write(f'-- Query {num}\n')
#     #         f.write('-- ' + '=' * 50 + '\n')
#     #         f.write(sql)
#     #         f.write('\n\n')
    
#     # print(f"✓ 保存为SQL文件: {sql_file}")
    
#     # 3.3 保存为独立的SQL文件（每个查询一个文件）
#     individual_dir = os.path.join(output_dir, "individual_queries")
#     if not os.path.exists(individual_dir):
#         os.makedirs(individual_dir)
    
#     for num, sql in sql_blocks:
#         query_file = os.path.join(individual_dir, f"query_{num:02d}.sql")
#         with open(query_file, 'w', encoding='utf-8') as f:
#             f.write(f'-- TPC-H Query {num}\n')
#             f.write('-- ' + '=' * 40 + '\n')
#             f.write(sql)
#             f.write('\n')
    
#     print(f"✓ 保存为独立查询文件: {individual_dir}/")
    
    # # 3.4 保存为JSON格式（可选）
    # import json
    # json_file = os.path.join(output_dir, "benchmark_queries.json")
    # queries_dict = {f"query_{num}": sql for num, sql in sql_blocks}
    # with open(json_file, 'w', encoding='utf-8') as f:
    #     json.dump(queries_dict, f, indent=2, ensure_ascii=False)
    
    # print(f"✓ 保存为JSON文件: {json_file}")
    
    
   
    
    # return sql_blocks
import glob

def load_sql_from_directory(sql_dir="cleaned_sql/individual_queries"):

    """从目录加载所有SQL文件"""
    sql_files = glob.glob(os.path.join(sql_dir, "*.sql"))
    sql_files.sort()  # 按文件名排序
    
    benchmark_sql_list = []
    
    for sql_file in sql_files:
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                sql_content = f.read().strip()
            
            # 获取查询编号
            filename = os.path.basename(sql_file)
            query_num = filename.replace('query_', '').replace('.sql', '')
            
            benchmark_sql_list.append({
                'file': sql_file,
                'number': query_num,
                'sql': sql_content
            })
            print(f"✓ 加载查询 {query_num}: {filename}")
            
        except Exception as e:
            print(f"✗ 加载失败 {sql_file}: {e}")
    
    print(f"总共加载 {len(benchmark_sql_list)} 个SQL查询")
    return benchmark_sql_list



def main():
    # ====== 1. 创建数据库对象 ======
    db = MySQLDriver(config={
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "947722",
        "database": "tpcch"
    })

    # ====== 2. 连接数据库 ======
    print("=== 连接数据库 ===")
    if not db.connect():
        print("数据库连接失败，程序退出")
        return

    # ====== 3. 删除旧 schema ======
    print("=== 删除旧数据库 ===")
    db.drop_schema()

    # ====== 4. 创建新 schema ======
    print("=== 创建新数据库 ===")
    if not db.create_schema():
        print("创建数据库失败")
        db.disconnect()
        return

    # 切换到新库（因为 drop+create 后连接仍使用旧库）
    db.connection.database = db.config["database"]

    # # ====== 5. 创建全部 TPC-C 表 ======
    # print("=== 创建 TPC-C 表结构 ===")
    # schema_sql_path = "./tpcc_schema.sql"    # 你自己的 schema 文件
    # if not os.path.exists(schema_sql_path):
    #     print(f"建表 SQL 文件不存在：{schema_sql_path}")
    #     db.disconnect()
    #     return
    
    # with open(schema_sql_path, "r", encoding="utf-8") as f:
    #     schema_sql = f.read()

    # for statement in schema_sql.split(";"):
    #     stmt = statement.strip()
    #     if stmt:
    #         if not db.execute_statement(stmt + ";"):
    #             print(f"创建表失败: {stmt}")
    #             db.disconnect()
    #             return
    tasks = {
        "warehouse": "D:/LLM4DBTuning/tpcc_data/WAREHOUSE.tbl",
        "district": "D:/LLM4DBTuning/tpcc_data/DISTRICT.tbl",
        "customer": "D:/LLM4DBTuning/tpcc_data/CUSTOMER.tbl",
        "history": "D:/LLM4DBTuning/tpcc_data/HISTORY.tbl",
        "order": "D:/LLM4DBTuning/tpcc_data/ORDER.tbl",
        "orderline": "D:/LLM4DBTuning/tpcc_data/ORDERLINE.tbl",
        "item": "D:/LLM4DBTuning/tpcc_data/ITEM.tbl",
        "stock": "D:/LLM4DBTuning/tpcc_data/STOCK.tbl",
        "nation": "D:/LLM4DBTuning/tpcc_data/NATION.tbl",
        "supplier": "D:/LLM4DBTuning/tpcc_data/SUPPLIER.tbl",
        "region": "D:/LLM4DBTuning/tpcc_data/REGION.tbl",
        "neworder": "D:/LLM4DBTuning/tpcc_data/NEWORDER.tbl"
    }


    print("=== 开始批量导入数据 ===")
    import_success = False
    for table, tbl_file in tasks.items():
        print(f"\n>>> 开始导入表 {table} 对应文件: {tbl_file}")

        if not os.path.exists(tbl_file):
            print(f"[跳过] 文件不存在: {tbl_file}")
            continue

        result = db.import_csv(table, tbl_file, delimiter="|")

        if result:
            import_success = True
            print(f"[成功] {table} 导入完成")
        else:
            print(f"[失败] {table} 导入失败")

    if import_success:
            # 3. 检查数据完整性
            integrity_ok = db.check_data_integrity(warehouse_count=1)
            
            if integrity_ok:
                print("\n🎉 TPC-C 数据准备完成！所有检查通过")
            else:
                print("\n⚠️  TPC-C 数据准备完成，但部分数据可能不完整")
        

    print("\n=== 全部导入任务执行完毕 ===")

    print("\n=== 开始进行测试性能任务 ===")
    print("\n=== 导入ch-benchmark 22条SQL ===")

    # 清洗sql
    # cleaned_sql_list = clean_and_save_sql("D:\LLM4DBTUNING\sql_queries\sql_queries.txt")

    benchmark_sql_list = load_sql_from_directory()
    print("\n=== 导入SQL成功 ===")

    iterations = 10  # 串行执行次数
    max_threads = os.cpu_count()                       # 最大线程数
    duration = 10      
    # 存放所有 SQL 测试结果
    all_results = []

    for query in benchmark_sql_list:
        sql_content = query['sql']  # 提取SQL字符串
        print(f"正在测试 Query {query['number']}: {sql_content[:50]}...")
        # 串行测试
        seq_result = db.evaluation(
            data="tpcch_data",
            physical_schema="tpcch",
            benchmark_sql=sql_content,
            iterations=iterations,
            concurrency=1  # 串行
        )
        
        # 并发测试
        conc_result = db.evaluation(
            data="tpcch_data",
            physical_schema="tpcch",
            benchmark_sql=sql_content,
            concurrency=max_threads,
            duration=duration
        )
        
        all_results.append({
            "sql": sql_content,
            "sequential": seq_result,
            "concurrent": conc_result
        })

    # 保存结果
    output_file = "tpcc_benchmark_results.json"

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=4)

    print(f"所有 SQL 测试结果已保存到 {output_file}")



if __name__ == "__main__":
    main()


# python MySQLDriver.py