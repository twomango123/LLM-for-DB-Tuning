from .base import SMO
# from ColumnMove import ColumnMove
# from ColumnCopy import ColumnCopy
# from ColumnRename import ColumnRename  
import pandas as pd
import os
import sqlglot
from sqlglot import expressions as exp
from log_info.log_info import get_logger

logger = get_logger()


class ColumnSplit(SMO):
    def __init__(self, table, old_column, new_columns, split_delimiter=None, split_position=None):
        self.table = table
        self.old_column = old_column
        self.new_columns = new_columns
        self.split_delimiter = split_delimiter
        self.split_position = split_position
        

    def apply_to_schema(self, db):