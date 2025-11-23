
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
            "item": 100000,
            "supplier": 10000,
            "nation": 62,
            "region": 5,
            
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