# smo/base.py

from abc import ABC, abstractmethod

class SMO(ABC):
    """
    所有 Schema Modification Operator 的抽象基类
    """
    @abstractmethod
    def apply_to_schema(self, schema):
        """用于更新数据库 schema"""
        pass

    @abstractmethod
    def apply_to_sql(self, sql_ast):
        """用于根据 SMO 改写 SQL"""
        pass

    @abstractmethod
    def apply_to_data(self, row):
        """
        输入 old tuple(row)
        输出 new tuple(row)
        """
        pass
    
