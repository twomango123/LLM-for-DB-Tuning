from DatabaseDriver import DatabaseDriver
from typing import List, Dict
"""
# 连接到 MySQL 数据库
conn = mysql.connector.connect(
    host="your_host",
    user="your_user",
    password="your_password",
    database="your_database"
)
# 创建游标
cursor = conn.cursor()
# 执行 SQL 查询
cursor.execute("SELECT * FROM your_table")
# 获取查询结果
result = cursor.fetchall()
# 输出结果
for row in result:
    print(row)
# 关闭游标和连接
cursor.close()
conn.close()
"""
'''
# 提交更改
conn.commit()'''

class MySQLDriver(DatabaseDriver):
    
    def connect(self) -> bool:
        try:
            import mysql.connector
            self.connection = mysql.connector.connect(**self.config)
            self.is_connected = True
            return True
        except Exception:
            return False
    
    def disconnect(self) -> bool:
        if self.connection:
            self.connection.close()
        self.is_connected = False
        return True
    
    def execute_query(self, query: str) -> List[Dict]:
        cursor = self.connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result

    def get_table_structure(self, table_name: str) -> Dict[str, Any]:
        """获取MySQL表结构"""
        query = """
        SELECT 
            COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE,
            COLUMN_KEY, EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = %s AND TABLE_SCHEMA = DATABASE()
        ORDER BY ORDINAL_POSITION
        """
        columns = self.execute_query(query, (table_name,))
        return {
            'table_name': table_name,
            'columns': columns
        }
    
