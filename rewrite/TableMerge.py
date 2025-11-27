import pandas as pd
from io import StringIO
import re
from base import SMO

class TableMerge(SMO):
    def __init__(self, old_tables, new_table, old_columns_list):
        """
        old_tables: 旧表名列表 [table1, table2]
        new_table: 新表名
        old_columns_list: 两个旧表的所有列名 [[table1_columns], [table2_columns]]
        """
        self.old_tables = old_tables
        self.new_table = new_table
        self.old_columns_list = old_columns_list
        self.conflict_columns = self._identify_conflict_columns()

       # 定义列名映射规则
        self.column_mapping = self._create_column_mapping()

    def apply_to_schema(self, schema):
        pass
    def apply_to_data(self, data_dict):
        """
        执行两表左连接，处理列名冲突
        data_dict: 包含表数据的字典 {table_name: dataframe}
        """
        if len(self.old_tables) != 2:
            raise ValueError("需要两个表进行连接")
        
        # 获取两个表的数据
        left_table = data_dict.get(self.old_tables[0])
        right_table = data_dict.get(self.old_tables[1])
        
        if left_table is None or right_table is None:
            raise ValueError(f"缺少表数据: {self.old_tables}")
        
        # 自动推断连接键
        join_key = self._infer_join_key(left_table, right_table)
        if not join_key:
            raise ValueError("无法推断连接键")
        
        # 执行左连接，为冲突列添加后缀
        merged_data = pd.merge(
            left_table, 
            right_table, 
            on=join_key, 
            how='left', 
            suffixes=('_left', '_right')
        )
        
        # 将连接后的数据添加到数据字典
        data_dict[self.new_table] = merged_data
        
        return data_dict


    def _identify_conflict_columns(self):
        """直接通过列名列表识别重复列"""
        if len(self.old_columns_list) != 2:
            return set()
        
        table1_columns = set(self.old_columns_list[0])
        table2_columns = set(self.old_columns_list[1])
        
        # 找出两个表中都存在的列名
        conflict_columns = table1_columns.intersection(table2_columns)
        return conflict_columns

    def _create_column_mapping(self):
        """创建列名映射规则"""
        column_mapping = {}
        
        # 左表列映射
        for col in self.old_columns_list[0]:
            if col in self.conflict_columns:
                column_mapping[(self.old_tables[0], col)] = f"{col}_left"
            else:
                column_mapping[(self.old_tables[0], col)] = col
        
        # 右表列映射
        for col in self.old_columns_list[1]:
            if col in self.conflict_columns:
                column_mapping[(self.old_tables[1], col)] = f"{col}_right"
            else:
                column_mapping[(self.old_tables[1], col)] = col
        
        return column_mapping

    def apply_to_sql(self, sql):
        """
        将涉及旧表的SQL重写为基于新大表的查询
        支持单表查询和多表连接查询
        """
        # 1. 分析查询类型（单表还是多表）
        query_type = self._analyze_query_type(sql)
        
        # 2. 解析SQL结构
        parsed_info = self._parse_sql_structure(sql)
        
        # 3. 替换表名为新表
        rewritten_sql = self._replace_table_references(sql, parsed_info, query_type)
        
        # 4. 处理列名冲突（基于已知的重复列名）
        rewritten_sql = self._resolve_column_conflicts(rewritten_sql, parsed_info, query_type)
        
        # 5. 处理聚合函数去重（仅多表连接时需要）
        if query_type == 'multi_table':
            rewritten_sql = self._handle_aggregation_distinct(rewritten_sql, parsed_info)
        
        # 6. 清理多余的JOIN条件（仅多表连接时需要）
        if query_type == 'multi_table':
            rewritten_sql = self._cleanup_join_conditions(rewritten_sql)
        
        return rewritten_sql

    def apply_to_data(self, data_dict):
        """
        应用表合并操作到数据
        data_dict: 包含旧表数据的字典 {table1: df1, table2: df2}
        返回合并后的新表数据
        """
        if len(self.old_tables) != 2 or len(data_dict) != 2:
            raise ValueError("需要两个表进行合并")
        
        # 获取两个表的数据
        table1_data = data_dict.get(self.old_tables[0])
        table2_data = data_dict.get(self.old_tables[1])
        
        if table1_data is None or table2_data is None:
            raise ValueError(f"缺少表数据: {self.old_tables}")
        
        # 执行左连接
        merged_data = self._perform_left_join(table1_data, table2_data)
        
        return merged_data

    def _perform_left_join(self, left_df, right_df):
        """
        执行左连接操作，处理列名冲突
        """
        # 识别连接键（自动推断或使用公共列）
        join_key = self._infer_join_key(left_df, right_df)
        
        if not join_key:
            raise ValueError("无法推断连接键，请明确指定连接条件")
        
        print(f"使用连接键: {join_key}")
        
        # 执行左连接，为冲突列添加后缀
        merged_df = pd.merge(
            left_df, 
            right_df, 
            on=join_key, 
            how='left', 
            suffixes=('_left', '_right')
        )
        
        return merged_df

    def _infer_join_key(self, left_df, right_df):
        """
        自动推断连接键
        """
        # 方法1: 查找名称相同的列
        common_columns = set(left_df.columns).intersection(set(right_df.columns))
        
        # 优先选择包含'id'、'key'等关键字的列
        potential_keys = []
        for col in common_columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['id', 'key', 'code', 'num']):
                potential_keys.append(col)
        
        if potential_keys:
            return potential_keys[0]  # 返回第一个可能的键
        
        # 方法2: 如果有多个公共列，选择第一个
        if common_columns:
            return list(common_columns)[0]
        
        return None

    def _analyze_query_type(self, sql):
        """
        分析查询类型：单表查询还是多表连接查询
        """
        sql_upper = sql.upper()
        
        # 检查是否包含JOIN关键字
        has_join = 'JOIN' in sql_upper
        
        # 检查FROM子句中是否有多个表（逗号分隔）
        from_pattern = r'FROM\s+([^,(]+)(?:\s*,\s*([^,(]+))?'
        from_match = re.search(from_pattern, sql, re.IGNORECASE)
        
        has_multiple_tables = False
        if from_match:
            table2 = from_match.group(2)
            has_multiple_tables = table2 is not None
        
        # 判断查询类型
        if has_join or has_multiple_tables:
            # 检查是否真的涉及多个旧表
            used_old_tables = []
            for table in self.old_tables:
                if re.search(rf'\b{re.escape(table)}\b', sql, re.IGNORECASE):
                    used_old_tables.append(table)
            
            if len(used_old_tables) >= 2:
                return 'multi_table'
        
        return 'single_table'

    def _parse_sql_structure(self, sql):
        """
        解析SQL结构，识别表引用、列引用、聚合函数等
        """
        parsed_info = {
            'table_aliases': {},  # 别名映射
            'column_references': [],  # 列引用
            'aggregations': [],  # 聚合函数
            'join_conditions': [],  # JOIN条件
            'used_tables': set()  # 使用的表
        }
        
        # 识别使用的表
        for table in self.old_tables:
            if re.search(rf'\b{re.escape(table)}\b', sql, re.IGNORECASE):
                parsed_info['used_tables'].add(table)
        
        # 识别表别名
        for table in self.old_tables:
            # 查找 "FROM table alias" 或 "JOIN table alias" 模式
            patterns = [
                rf'FROM\s+{re.escape(table)}\s+(\w+)',
                rf'JOIN\s+{re.escape(table)}\s+(\w+)',
                rf',\s*{re.escape(table)}\s+(\w+)'
            ]
            for pattern in patterns:
                matches = re.finditer(pattern, sql, re.IGNORECASE)
                for match in matches:
                    parsed_info['table_aliases'][match.group(1)] = table
        
        # 如果没有别名，使用表名作为别名
        for table in self.old_tables:
            if table in parsed_info['used_tables'] and table not in parsed_info['table_aliases'].values():
                parsed_info['table_aliases'][table] = table
        
        # 识别列引用 (table.column 或 alias.column)
        column_pattern = r'(\w+)\.(\w+)'
        matches = re.finditer(column_pattern, sql)
        for match in matches:
            table_or_alias = match.group(1)
            column = match.group(2)
            actual_table = self._get_actual_table(table_or_alias, parsed_info['table_aliases'])
            
            parsed_info['column_references'].append({
                'table_or_alias': table_or_alias,
                'column': column,
                'full_reference': match.group(0),
                'is_conflict': column in self.conflict_columns,
                'actual_table': actual_table,
                'new_column_name': self._get_new_column_name(actual_table, column)
            })
        
        # 识别无表前缀的列引用（在单表查询中常见）
        if len(parsed_info['used_tables']) == 1:
            table_name = list(parsed_info['used_tables'])[0]
            # 查找没有被表前缀的列名（简单的列名）
            simple_column_pattern = r'\b(\w+)\b(?=\s*(?:,|FROM|WHERE|GROUP BY|ORDER BY|HAVING|$))'
            # 排除SQL关键字
            sql_keywords = {'SELECT', 'FROM', 'WHERE', 'GROUP', 'BY', 'ORDER', 'HAVING', 'AND', 'OR', 'AS'}
            
            matches = re.finditer(simple_column_pattern, sql, re.IGNORECASE)
            for match in matches:
                column = match.group(1)
                if (column.upper() not in sql_keywords and 
                    not column.isdigit() and 
                    column in self.old_columns_list[0] + self.old_columns_list[1]):
                    
                    parsed_info['column_references'].append({
                        'table_or_alias': None,  # 无表前缀
                        'column': column,
                        'full_reference': column,
                        'is_conflict': column in self.conflict_columns,
                        'actual_table': table_name,
                        'new_column_name': self._get_new_column_name(table_name, column)
                    })
        
        # 识别聚合函数
        agg_patterns = [
            (r'COUNT\s*\(\s*(.*?)\s*\)', 'COUNT'),
            (r'SUM\s*\(\s*(.*?)\s*\)', 'SUM'),
            (r'AVG\s*\(\s*(.*?)\s*\)', 'AVG'),
            (r'MIN\s*\(\s*(.*?)\s*\)', 'MIN'),
            (r'MAX\s*\(\s*(.*?)\s*\)', 'MAX')
        ]
        
        for pattern, func_name in agg_patterns:
            matches = re.finditer(pattern, sql, re.IGNORECASE)
            for match in matches:
                parsed_info['aggregations'].append({
                    'function': func_name,
                    'expression': match.group(1),
                    'full_match': match.group(0)
                })
        
        return parsed_info

    def _get_actual_table(self, table_or_alias, table_aliases):
        """获取实际的表名"""
        if table_or_alias in self.old_tables:
            return table_or_alias
        elif table_or_alias in table_aliases:
            return table_aliases[table_or_alias]
        return None

    def _get_new_column_name(self, table_name, column_name):
        """获取新表中的列名"""
        if table_name and column_name:
            return self.column_mapping.get((table_name, column_name), column_name)
        return column_name

    def _replace_table_references(self, sql, parsed_info, query_type):
        """
        将所有旧表引用替换为新表引用
        """
        # 构建表别名映射
        self.table_aliases_map = parsed_info['table_aliases']
        
        if query_type == 'single_table':
            # 单表查询：直接替换表名
            for table in parsed_info['used_tables']:
                # 替换FROM子句中的表名
                from_pattern = rf'FROM\s+{re.escape(table)}(?:\s+(\w+))?'
                def from_replacer(match):
                    alias = match.group(1)
                    if alias:
                        return f"FROM {self.new_table} {alias}"
                    else:
                        return f"FROM {self.new_table}"
                
                sql = re.sub(from_pattern, from_replacer, sql, flags=re.IGNORECASE)
                
        else:  # multi_table
            # 多表查询：替换FROM中的第一个表，移除其他表
            from_pattern = r'FROM\s+([^,\s]+)(?:\s+(\w+))?'
            def from_replacer(match):
                table_name = match.group(1)
                if table_name in self.old_tables:
                    return f"FROM {self.new_table}"
                return match.group(0)
            
            sql = re.sub(from_pattern, from_replacer, sql, flags=re.IGNORECASE)
            
            # 移除其他表引用
            for table in self.old_tables[1:]:
                join_pattern = rf'\s+(?:LEFT\s+)?JOIN\s+{re.escape(table)}(?:\s+\w+)?\s+ON\s+[^)]+\)?'
                sql = re.sub(join_pattern, '', sql, flags=re.IGNORECASE)
                
                comma_pattern = rf',\s*{re.escape(table)}(?:\s+\w+)?'
                sql = re.sub(comma_pattern, '', sql, flags=re.IGNORECASE)
        
        return sql

    def _resolve_column_conflicts(self, sql, parsed_info, query_type):
        """
        处理列名冲突 - 基于已知的重复列名
        """
        # 按照引用长度从长到短排序，避免部分替换
        column_refs_sorted = sorted(parsed_info['column_references'], 
                                  key=lambda x: len(x['full_reference']), 
                                  reverse=True)
        
        for ref in column_refs_sorted:
            old_ref = ref['full_reference']
            new_column_name = ref['new_column_name']
            
            if new_column_name != ref['column']:  # 只有当列名发生变化时才替换
                if ref['table_or_alias']:  # 有表前缀的列
                    new_ref = f"{self.new_table}.{new_column_name}"
                else:  # 无表前缀的列（单表查询）
                    new_ref = new_column_name
                
                # 精确替换，避免部分匹配
                sql = re.sub(rf'\b{re.escape(old_ref)}\b', new_ref, sql)
        
        return sql

    def _handle_aggregation_distinct(self, sql, parsed_info):
        """
        处理聚合函数的去重问题（仅多表连接时需要）
        """
        def count_replacer(match):
            full_count = match.group(0)
            count_expr = match.group(1)
            
            if 'DISTINCT' in count_expr.upper():
                return full_count
            
            if count_expr.strip() == '*':
                return full_count
            
            # 对于COUNT(column)，左连接后需要去重
            involves_right_table = False
            for table_alias in self.table_aliases_map:
                if self.table_aliases_map[table_alias] == self.old_tables[1]:
                    if table_alias in count_expr:
                        involves_right_table = True
                        break
            
            if involves_right_table or any(col in count_expr for col in self.conflict_columns):
                return f"COUNT(DISTINCT {count_expr})"
            else:
                return full_count
        
        sql = re.sub(r'COUNT\s*\(\s*(.*?)\s*\)', count_replacer, sql, flags=re.IGNORECASE)
        
        return sql

    def _cleanup_join_conditions(self, sql):
        """
        清理多余的JOIN条件（仅多表连接时需要）
        """
        where_pattern = r'WHERE\s+(.*?)(?=\s+(GROUP BY|ORDER BY|HAVING|LIMIT|\s*$))'
        
        def where_cleaner(match):
            where_clause = match.group(1)
            
            join_patterns = [
                rf'{self.old_tables[0]}\.\w+\s*=\s*{self.old_tables[1]}\.\w+',
                rf'{self.old_tables[1]}\.\w+\s*=\s*{self.old_tables[0]}\.\w+',
            ]
            
            for pattern in join_patterns:
                where_clause = re.sub(pattern, '1=1', where_clause)
            
            where_clause = re.sub(r'\s+AND\s+1=1', '', where_clause)
            where_clause = re.sub(r'1=1\s+AND\s+', '', where_clause)
            where_clause = re.sub(r'^\s*1=1\s*$', '', where_clause)
            
            if where_clause.strip() and where_clause.strip() != '1=1':
                return f"WHERE {where_clause}"
            else:
                return ""
        
        sql = re.sub(where_pattern, where_cleaner, sql, flags=re.IGNORECASE)
        
        return sql

def test_conflict_columns():
    """
    专门测试冲突列的处理
    """
    print("=== 冲突列处理测试 ===")
    
    # 创建示例数据
    users_data = """
    user_id,user_name,city,created_at,status
    1,张三,北京,2024-01-01,active
    2,李四,上海,2024-01-02,active  
    """
    
    users_df = pd.read_csv(StringIO(users_data.strip()))
    users_columns = list(users_df.columns)
    
    orders_data = """
    order_id,user_id,product_name,amount,order_date,status
    101,1,手机,2999.00,2024-01-15,completed
    102,1,耳机,399.00,2024-01-16,completed
    """
    
    orders_df = pd.read_csv(StringIO(orders_data.strip()))
    orders_columns = list(orders_df.columns)
    
    # 创建TableMerge实例
    table_merge = TableMerge(
        old_tables=['users', 'orders'], 
        new_table='user_orders_joined',
        old_columns_list=[users_columns, orders_columns]
    )
    
    print(f"识别出的冲突列: {table_merge.conflict_columns}")
    print(f"列名映射: {table_merge.column_mapping}")
    print()
    
    # 测试单表查询 - users表
    test_cases = [
        {
            'name': '单表查询 - users表(冲突列status)',
            'sql': """
            SELECT 
                user_id,
                user_name,
                status
            FROM users
            WHERE status = 'active'
            """
        },
        {
            'name': '单表查询 - orders表(冲突列status)',
            'sql': """
            SELECT 
                order_id,
                user_id,
                status
            FROM orders
            WHERE status = 'completed'
            """
        },
        {
            'name': '单表查询 - users表(无表前缀)',
            'sql': """
            SELECT 
                user_id,
                user_name,
                status
            FROM users
            WHERE city = '北京'
            """
        },
        {
            'name': '多表连接查询',
            'sql': """
            SELECT 
                u.user_id,
                u.status as user_status,
                o.status as order_status
            FROM users u
            LEFT JOIN orders o ON u.user_id = o.user_id
            """
        }
    ]
    
    for test_case in test_cases:
        print(f"测试: {test_case['name']}")
        print("原始SQL:")
        print(test_case['sql'])
        
        rewritten_sql = table_merge.apply_to_sql(test_case['sql'])
        print("重写后SQL:")
        print(rewritten_sql)
        print("-" * 50)

if __name__ == "__main__":
    test_conflict_columns()