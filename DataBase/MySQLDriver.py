import mysql.connector
from mysql.connector import errorcode
from DataBase.DatabaseDriver import DatabaseDriver
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

    

