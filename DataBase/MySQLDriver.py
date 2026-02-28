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
        # 使用 buffered=True 以确保读取了所有结果集，避免后续出现
        # "Unread result found" 错误（尤其是包含 SELECT 的 PREPARE/EXECUTE 场景）。
        cursor = self.connection.cursor(dictionary=True, buffered=True)
        try:
            cursor.execute(query)
            rows = cursor.fetchall() if cursor.with_rows else []
            # 消耗可能存在的后续结果集（防御性处理）
            while True:
                more = cursor.nextset()
                if not more:
                    break
                if cursor.with_rows:
                    _ = cursor.fetchall()
            return rows
        finally:
            cursor.close()

    def execute_statement(self, statement: str) -> bool:
        if not self.is_connected:
            raise RuntimeError("数据库未连接")
        try:
            # buffered=True 确保任何结果集（例如 EXECUTE 执行到 SELECT 1）被消费
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(statement)
            # 若该语句产生结果集（例如 SELECT），需将其读取完毕，否则
            # mysql-connector 会在下一条语句时报 "Unread result found"
            if cursor.with_rows:
                _ = cursor.fetchall()
            # 消耗可能存在的后续结果集（极少见，但做防御）
            while True:
                more = cursor.nextset()
                if not more:
                    break
                if cursor.with_rows:
                    _ = cursor.fetchall()
            self.connection.commit()
            cursor.close()
            return True
        except mysql.connector.Error as err:
            print(f"执行语句失败: {err}\nSQL: {statement}")
            return False

    def drop_schema(self) -> bool:
        dbname = self.config.get("database", "tpcch")
        return self.execute_statement(f"DROP DATABASE IF EXISTS {dbname}")

    
