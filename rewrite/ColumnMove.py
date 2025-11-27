import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Function, TokenList
from sqlparse.tokens import Keyword, DML, Name, Punctuation
import csv
from pathlib import Path

# 列移动
class ColumnMove:
    """
    Move column: source_table.source_column -> target_table.target_column
    SQL rewrite implemented with sqlglot.
    """

    def __init__(self, source_table, source_column, target_table, target_column, source_fp, target_fp, column_name, sep='|'):
        # keep bare table names
        self.source_table = source_table.split(".")[-1]
        self.source_column = source_column
        self.target_table = target_table.split(".")[-1]
        self.target_column = target_column
        self.source_fp = source_fp
        self.target_fp = target_fp
        self.column_name = column_name
        self.sep = sep

    
    def apply_to_sql(self, sql: str) -> str:
        parsed = sqlparse.parse(sql)
        new_statements = []

        for stmt in parsed:
            # 1. 替换列
            self._replace_columns(stmt)
            # 2. 检查并加入新表（包括子查询）
            self._add_table_if_missing(stmt)
            new_statements.append(str(stmt))

        return " ".join(new_statements)
    
    def apply_to_data(self):
        
        # 1. 读取原表
        self.source_fp.seek(0)
        reader = csv.DictReader(self.source_fp, delimiter=self.sep)
        rows = list(reader)
        fieldnames = reader.fieldnames

        if self.column_name not in fieldnames:
            raise ValueError(f"Column {self.column_name} not in source table")

        # 2. 读取目标表已有内容
        self.target_fp.seek(0)
        try:
            target_reader = csv.DictReader(self.target_fp, delimiter=self.sep)
            target_fieldnames = target_reader.fieldnames or []
            target_rows = list(target_reader)
        except csv.Error:
            target_fieldnames = []
            target_rows = []

        # 3. 复制列到目标表
        for i, row in enumerate(rows):
            value = row[self.column_name]
            if i < len(target_rows):
                target_rows[i][self.column_name] = value
            else:
                target_rows.append({self.column_name: value})

        if self.column_name not in target_fieldnames:
            target_fieldnames.append(self.column_name)

        # 写入目标表
        self.target_fp.seek(0)
        self.target_fp.truncate(0)
        writer = csv.DictWriter(self.target_fp, fieldnames=target_fieldnames, delimiter=self.sep)
        writer.writeheader()
        for row in target_rows:
            for col in target_fieldnames:
                if col not in row:
                    row[col] = ''
            writer.writerow(row)

        # 4. 删除原表列
        new_fieldnames = [col for col in fieldnames if col != self.column_name]
        self.source_fp.seek(0)
        self.source_fp.truncate(0)
        writer = csv.DictWriter(self.source_fp, fieldnames=new_fieldnames, delimiter=self.sep)
        writer.writeheader()
        for row in rows:
            new_row = {col: row[col] for col in new_fieldnames}
            writer.writerow(new_row)

        print(f"Column {self.column_name} moved successfully.")

    def _replace_columns(self, token_list):
        """Recursively replace columns (bare or table-prefixed)"""
        for token in token_list.tokens:
            if token.is_group:
                self._replace_columns(token)
            elif isinstance(token, Identifier):
                self._replace_identifier(token)
            elif isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    self._replace_identifier(identifier)
            elif token.ttype is Name:
                parts = token.value.split(".")
                col_name = parts[-1].lower()
                if col_name == self.source_column.lower():
                    token.value = self.target_column

    def _replace_identifier(self, identifier):
        if isinstance(identifier, Function):
            for token in identifier.tokens:
                if token.is_group:
                    self._replace_columns(token)
        elif isinstance(identifier, Identifier):
            # 直接取最后一个 token 作为列名
            if identifier.tokens:
                last_token = identifier.tokens[-1]
                if last_token.ttype in (Name,) and last_token.value.lower() == self.source_column.lower():
                    last_token.value = self.target_column

    def _add_table_if_missing(self, stmt):
    
        def process(token_list):
            if not isinstance(token_list, TokenList):
                return False

            has_column_to_replace = False

            # 先递归检查子token是否有需要替换的列
            for token in token_list.tokens:
                if token.is_group:
                    if process(token):
                        has_column_to_replace = True
                elif token.ttype is Name:
                    parts = token.value.split(".")
                    col_name = parts[-1].lower()
                    if col_name == self.source_column.lower():
                        has_column_to_replace = True
                elif isinstance(token, Identifier):
                    last_token = token.tokens[-1] if token.tokens else None
                    if last_token and last_token.ttype is Name and last_token.value.lower() == self.source_column.lower():
                        has_column_to_replace = True
                elif isinstance(token, IdentifierList):
                    for ident in token.get_identifiers():
                        last_token = ident.tokens[-1] if ident.tokens else None
                        if last_token and last_token.ttype is Name and last_token.value.lower() == self.source_column.lower():
                            has_column_to_replace = True

            # 如果当前 token_list 有需要替换的列，处理 FROM/JOIN
            if has_column_to_replace:
                for i, token in enumerate(token_list.tokens):
                    if token.ttype is Keyword and token.value.upper() in ("FROM", "JOIN"):
                        j = i + 1
                        while j < len(token_list.tokens) and token_list.tokens[j].ttype in (sqlparse.tokens.Whitespace, sqlparse.tokens.Newline):
                            j += 1
                        if j < len(token_list.tokens):
                            next_token = token_list.tokens[j]
                            if isinstance(next_token, IdentifierList):
                                names = [t.value.split(".")[-1] for t in next_token.get_identifiers()]
                            else:
                                names = [next_token.value.split(".")[-1]]

                            if self.target_table not in names:
                                if isinstance(next_token, IdentifierList):
                                    next_token.tokens.append(sqlparse.sql.Token(Punctuation, ","))
                                    next_token.tokens.append(Identifier([sqlparse.sql.Token(Name, self.target_table)]))
                                else:
                                    token_list.tokens[j] = IdentifierList([
                                        next_token,
                                        sqlparse.sql.Token(Punctuation, ","),
                                        Identifier([sqlparse.sql.Token(Name, self.target_table)])
                                    ])
            return has_column_to_replace

        process(stmt)

# 使用示例
  
sql = """
select
	n_name,
	sum(ol_amount) as revenue
from
	tpcch.customer, tpcch.order, tpcch.orderline, tpcch.stock, tpcch.supplier, tpcch.nation, tpcch.region
where
		c_id = o_c_id
	and c_w_id = o_w_id
	and c_d_id = o_d_id
	and ol_o_id = o_id
	and ol_w_id = o_w_id
	and ol_d_id=o_d_id
	and ol_w_id = s_w_id
	and ol_i_id = s_i_id
	and s_su_suppkey = su_suppkey
	and c_n_nationkey = su_nationkey
	and su_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and o_entry_d >= '2007-01-02 00:00:00.000000'
group by
		n_name
order by
	revenue desc;
"""
source_path = "../tpcc_data/NEW_CUSTOMER.tbl"
target_path = "../tpcc_data/CUSTOMER.tbl"

# rewrite data
rewrite = ColumnMove("customer", "c_id", "cc", "cc_id", source_path, target_path, "c_balance")
print(rewrite.apply_to_sql(sql))


# rewrite slq
# 运行完需要把源文件的列恢复，c_id是第一列
# 打开文件
with open(source_path, 'r+', newline='') as source_fp, open(target_path, 'a+', newline='') as target_fp:
    mover = ColumnMove("customer", "c_id", "cc", "cc_id", source_fp, target_fp, "C_ID")
    mover.apply_to_data()