import csv
import os
import time
from typing import List, Dict

class CHDataLoader:
    """CH-benchmark数据加载器"""
    
    def __init__(self, db_driver):
        self.db_driver = db_driver
    
    def create_tables(self):
        """创建CH-benchmark表结构"""
        create_queries = [
            # Warehouse表
            """
            CREATE TABLE IF NOT EXISTS warehouse (
                w_id INT PRIMARY KEY,
                w_name VARCHAR(10),
                w_street_1 VARCHAR(20),
                w_street_2 VARCHAR(20),
                w_city VARCHAR(20),
                w_state CHAR(2),
                w_zip CHAR(9),
                w_tax DECIMAL(4,4),
                w_ytd DECIMAL(12,2)
            )
            """,
            # District表
            """
            CREATE TABLE IF NOT EXISTS district (
                d_id INT,
                d_w_id INT,
                d_name VARCHAR(10),
                d_street_1 VARCHAR(20),
                d_street_2 VARCHAR(20),
                d_city VARCHAR(20),
                d_state CHAR(2),
                d_zip CHAR(9),
                d_tax DECIMAL(4,4),
                d_ytd DECIMAL(12,2),
                d_next_o_id INT,
                PRIMARY KEY (d_w_id, d_id)
            )
            """,
            # Customer表
            """
            CREATE TABLE IF NOT EXISTS customer (
                c_id INT,
                c_d_id INT,
                c_w_id INT,
                c_first VARCHAR(16),
                c_middle CHAR(2),
                c_last VARCHAR(16),
                c_street_1 VARCHAR(20),
                c_street_2 VARCHAR(20),
                c_city VARCHAR(20),
                c_state CHAR(2),
                c_zip CHAR(9),
                c_phone CHAR(16),
                c_since TIMESTAMP,
                c_credit CHAR(2),
                c_credit_lim DECIMAL(12,2),
                c_discount DECIMAL(4,4),
                c_balance DECIMAL(12,2),
                c_ytd_payment DECIMAL(12,2),
                c_payment_cnt INT,
                c_delivery_cnt INT,
                c_data VARCHAR(500),
                PRIMARY KEY (c_w_id, c_d_id, c_id)
            )
            """,
            # 继续添加其他表...
            """
            CREATE TABLE IF NOT EXISTS orders (
                o_id INT,
                o_d_id INT,
                o_w_id INT,
                o_c_id INT,
                o_entry_d TIMESTAMP,
                o_carrier_id INT,
                o_ol_cnt DECIMAL(2,0),
                o_all_local DECIMAL(1,0),
                PRIMARY KEY (o_w_id, o_d_id, o_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS order_line (
                ol_o_id INT,
                ol_d_id INT,
                ol_w_id INT,
                ol_number INT,
                ol_i_id INT,
                ol_supply_w_id INT,
                ol_delivery_d TIMESTAMP,
                ol_quantity DECIMAL(2,0),
                ol_amount DECIMAL(6,2),
                ol_dist_info CHAR(24),
                PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS item (
                i_id INT PRIMARY KEY,
                i_im_id INT,
                i_name VARCHAR(24),
                i_price DECIMAL(5,2),
                i_data VARCHAR(50)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS stock (
                s_i_id INT,
                s_w_id INT,
                s_quantity DECIMAL(4,0),
                s_dist_01 CHAR(24),
                s_dist_02 CHAR(24),
                s_dist_03 CHAR(24),
                s_dist_04 CHAR(24),
                s_dist_05 CHAR(24),
                s_dist_06 CHAR(24),
                s_dist_07 CHAR(24),
                s_dist_08 CHAR(24),
                s_dist_09 CHAR(24),
                s_dist_10 CHAR(24),
                s_ytd DECIMAL(8,0),
                s_order_cnt INT,
                s_remote_cnt INT,
                s_data VARCHAR(50),
                PRIMARY KEY (s_w_id, s_i_id)
            )
            """
        ]
        
        print("创建表结构...")
        for i, query in enumerate(create_queries, 1):
            try:
                self.db_driver.execute_query(query)
                print(f"创建表 {i}/{len(create_queries)} 完成")
            except Exception as e:
                print(f"创建表失败: {e}")
    
    def load_table_data(self, table_name: str, csv_file_path: str, batch_size: int = 1000):
        """加载单个表的数据"""
        if not os.path.exists(csv_file_path):
            print(f"CSV文件不存在: {csv_file_path}")
            return False
        
        print(f"加载表 {table_name} 从 {csv_file_path}...")
        start_time = time.time()
        loaded_rows = 0
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                headers = next(reader)  # 读取标题行
                
                # 构建INSERT语句
                placeholders = ', '.join(['%s'] * len(headers))
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
                
                batch = []
                for row in reader:
                    # 处理空值和数据类型转换
                    processed_row = []
                    for value in row:
                        if value == '':
                            processed_row.append(None)
                        else:
                            processed_row.append(value)
                    
                    batch.append(processed_row)
                    loaded_rows += 1
                    
                    # 批量插入
                    if len(batch) >= batch_size:
                        self._execute_batch_insert(insert_sql, batch)
                        batch = []
                        print(f"  已加载 {loaded_rows} 行...")
                
                # 插入剩余数据
                if batch:
                    self._execute_batch_insert(insert_sql, batch)
            
            elapsed_time = time.time() - start_time
            print(f"表 {table_name} 加载完成: {loaded_rows} 行, 耗时 {elapsed_time:.2f} 秒")
            return True
            
        except Exception as e:
            print(f"加载表 {table_name} 失败: {e}")
            return False
    
    def _execute_batch_insert(self, insert_sql: str, batch: List[List]):
        """执行批量插入"""
        try:
            # 对于MySQL，可以使用executemany
            self.db_driver.execute_query("START TRANSACTION")
            for row in batch:
                # 构建参数化查询
                formatted_sql = insert_sql
                for i, value in enumerate(row):
                    if value is None:
                        formatted_sql = formatted_sql.replace('%s', 'NULL', 1)
                    else:
                        # 转义特殊字符
                        escaped_value = str(value).replace("'", "''")
                        formatted_sql = formatted_sql.replace('%s', f"'{escaped_value}'", 1)
                
                self.db_driver.execute_query(formatted_sql)
            
            self.db_driver.execute_query("COMMIT")
            
        except Exception as e:
            self.db_driver.execute_query("ROLLBACK")
            raise e
    
    def load_all_data(self, csv_directory: str):
        """加载所有CH-benchmark数据"""
        table_files = {
            'warehouse': 'warehouse.csv',
            'district': 'district.csv',
            'customer': 'customer.csv', 
            'history': 'history.csv',
            'new_orders': 'new_orders.csv',
            'orders': 'orders.csv',
            'order_line': 'order_line.csv',
            'item': 'item.csv',
            'stock': 'stock.csv'
        }
        
        print(f"从目录加载数据: {csv_directory}")
        
        for table_name, csv_file in table_files.items():
            csv_path = os.path.join(csv_directory, csv_file)
            self.load_table_data(table_name, csv_path)
    
    def create_indexes(self):
        """创建性能测试所需的索引"""
        index_queries = [
            "CREATE INDEX idx_customer_name ON customer(c_last, c_first)",
            "CREATE INDEX idx_orders_customer ON orders(o_c_id, o_d_id, o_w_id)",
            "CREATE INDEX idx_orders_date ON orders(o_entry_d)",
            "CREATE INDEX idx_order_line_order ON order_line(ol_o_id, ol_d_id, ol_w_id)",
            "CREATE INDEX idx_stock_item ON stock(s_i_id)",
        ]
        
        print("创建索引...")
        for query in index_queries:
            try:
                self.db_driver.execute_query(query)
                print(f"创建索引: {query.split()[2]}")
            except Exception as e:
                print(f"创建索引失败: {e}")