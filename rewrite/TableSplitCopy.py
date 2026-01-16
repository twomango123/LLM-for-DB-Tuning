try:
    from .base import SMO, MySQLConstraintHelper
    # ColumnMove/ColumnCopy 已移除
    from .ColumnRename import ColumnRename
except Exception:  # pragma: no cover
    from base import SMO, MySQLConstraintHelper
    # ColumnMove/ColumnCopy 已移除
    from ColumnRename import ColumnRename
try:
    import pandas as pd  # 可选依赖，仅在 apply_to_data 使用
except Exception:
    pd = None
import os

class TableSplit(SMO):
    def __init__(self, old_table, new_tables, columnList, primary_keys_dict):
        self.old_table = old_table
        self.new_tables = new_tables  # 新表名列表
        self.columnList = columnList  # 每个新表对应的列名列表
        self.primary_keys_dict = primary_keys_dict  # 每个新表的主键列名

    def apply_to_schema(self, db=None):
        """
        基于“主键表 + 业务表”模式落地实体表：
        - 创建 {old}_keys 主键表（去重）
        - 针对每个新表创建包含主键+业务列的去重表
        - 可选择不立即删除旧表（此处不删，避免破坏）。

        如果传入 db 则执行并返回 bool，否则返回 SQL 脚本字符串。
        """
        old = self.old_table
        pk_table = f"{old}_keys"
        # 汇总所有主键列
        all_pks = []
        for _, pks in (self.primary_keys_dict or {}).items():
            for p in pks:
                if p not in all_pks:
                    all_pks.append(p)

        stmts = []
        stmts.append('SET FOREIGN_KEY_CHECKS=0')
        if all_pks:
            pk_cols = ", ".join(all_pks)
            stmts.append(
                f"CREATE TABLE `{pk_table}` AS SELECT DISTINCT {pk_cols} FROM `{old}`;"
            )
            stmts.append(f"ALTER TABLE `{pk_table}` ADD PRIMARY KEY ({pk_cols});")

        # 业务表
        for i, new_table in enumerate(self.new_tables):
            table_pks = self.primary_keys_dict.get(new_table, [])
            biz_cols = self.columnList[i] if i < len(self.columnList) else []
            # 去除和主键重复的列
            cols = table_pks + [c for c in biz_cols if c not in table_pks]
            cols_str = ", ".join(f"`{c}`" for c in cols)
            stmts.append(
                f"CREATE TABLE `{new_table}` AS SELECT DISTINCT {cols_str} FROM `{old}`;"
            )
            if table_pks:
                pk_str = ", ".join(f"`{c}`" for c in table_pks)
                stmts.append(f"ALTER TABLE `{new_table}` ADD PRIMARY KEY ({pk_str});")

        # 使用约束助手迁移唯一约束、出站外键等（避免重复添加主键）
        if db is not None:
            helper = MySQLConstraintHelper(db)
            constraints = helper.fetch_constraints(old)
            for i, new_table in enumerate(self.new_tables):
                table_pks = self.primary_keys_dict.get(new_table, [])
                biz_cols = self.columnList[i] if i < len(self.columnList) else []
                include_cols = list(dict.fromkeys(table_pks + biz_cols))
                add_stmts = helper.build_add_constraints_for_table(new_table, constraints, include_cols, rename_map=None)
                # 过滤掉可能重复的 ADD PRIMARY KEY 语句
                filtered = [s for s in add_stmts if ' ADD PRIMARY KEY ' not in s]
                stmts.extend(filtered)

            # 入站外键：若引用列是旧表主键子集，则改指向主键表
            inbound_fix = helper.build_update_inbound_fks(old, pk_table, all_pks)
            stmts.extend(inbound_fix)

        stmts.append('SET FOREIGN_KEY_CHECKS=1')

        script = "\n".join(stmts)
        if db is not None and hasattr(db, "execute_statement"):
            ok = True
            for s in stmts:
                ok = ok and db.execute_statement(s)
            return ok
        return script

    # ----------------------------------------------------------
    # SQL 改写操作
    # # ----------------------------------------------------------
    # 这个逻辑还不够完善，第一个需要考虑*代表查询表中所有列，第二点一旦遇到某个查询改写后包含多个原表拆分来的新表，需要增加新表连接操作才能确保查询到的数据和之前查询原表得到的一致，注意。
    def apply_to_sql(self, sql):
        
        """
        重写SQL字符串 - 使用主键表进行连接
        """
        import re
        
        # 构建列到表的映射
        column_to_table = {}
        for i, table in enumerate(self.new_tables):
            # 该表的主键列
            pk_columns = self.primary_keys_dict.get(table, [])
            # 该表的业务列
            business_columns = self.columnList[i] if i < len(self.columnList) else []
            
            # 记录每个列对应的表
            for col in pk_columns + business_columns:
                if col not in column_to_table:
                    column_to_table[col] = []
                if table not in column_to_table[col]:
                    column_to_table[col].append(table)
        
        print(f"  [DEBUG] 列映射: {column_to_table}")
        
        # 主键表名
        pk_table_name = f"{self.old_table}_keys"
        
        # 简单的SQL重写逻辑
        sql_upper = sql.upper()
        
        # 1. 处理SELECT * 的情况
        if "SELECT *" in sql_upper:
            # 展开SELECT * 为所有列（通过主键表JOIN所有业务表）
            all_columns = []
            for table in self.new_tables:
                pk_columns = self.primary_keys_dict.get(table, [])
                business_columns = self.columnList[self.new_tables.index(table)] if self.new_tables.index(table) < len(self.columnList) else []
                for col in pk_columns + business_columns:
                    # 避免重复列，使用业务表的列
                    if col not in all_columns:
                        all_columns.append(f"{table}.{col}")
            
            sql = sql.replace("SELECT *", f"SELECT {', '.join(all_columns)}")
        
        # 2. 分析查询中实际使用的列
        used_columns = set()
        
        # 从SELECT子句中提取列
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_upper, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_clause = select_match.group(1)
            if select_clause != "*":
                # 解析列名（简化处理）
                columns = [col.strip().split(' ')[0].split('.')[-1] for col in select_clause.split(',')]
                used_columns.update(columns)
        
        # 从WHERE子句中提取列
        where_match = re.search(r'WHERE\s+(.*?)(?:\s+ORDER BY|\s+GROUP BY|$)', sql_upper, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            # 简单的列名提取
            words = re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', where_clause)
            for word in words:
                if word.upper() not in ['AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS', 'NULL', 'TRUE', 'FALSE']:
                    if word in column_to_table:
                        used_columns.add(word)
        
        print(f"  [DEBUG] 使用的列: {used_columns}")
        
        # 3. 确定需要哪些业务表
        needed_business_tables = set()
        for col in used_columns:
            if col in column_to_table:
                needed_business_tables.update(column_to_table[col])
        
        # 如果没有明确使用的列，使用所有业务表
        if not needed_business_tables:
            needed_business_tables = set(self.new_tables)
        
        needed_business_tables = list(needed_business_tables)
        print(f"  [DEBUG] 需要的业务表: {needed_business_tables}")
        
        # 4. 构建SQL重写逻辑
        if len(needed_business_tables) == 0:
            # 没有业务表需要，直接使用主键表
            new_sql = re.sub(r'\b' + re.escape(self.old_table) + r'\b', pk_table_name, sql, flags=re.IGNORECASE)
        
        elif len(needed_business_tables) == 1:
            # 只有一个业务表，直接使用该表
            business_table = needed_business_tables[0]
            new_sql = re.sub(r'\b' + re.escape(self.old_table) + r'\b', business_table, sql, flags=re.IGNORECASE)
        
        else:
            # 多个业务表，使用主键表进行JOIN
            base_table = pk_table_name
            
            # 替换FROM子句
            from_pattern = re.compile(r'FROM\s+' + re.escape(self.old_table) + r'\b', re.IGNORECASE)
            new_from = f"FROM {base_table}"
            
            # 为每个需要的业务表添加JOIN
            join_clauses = []
            for business_table in needed_business_tables:
                # 获取该业务表的主键列
                table_pk = self.primary_keys_dict.get(business_table, [])
                if table_pk:
                    # 构建JOIN条件：主键表.pk = 业务表.pk
                    join_conditions = [f"{base_table}.{pk} = {business_table}.{pk}" for pk in table_pk]
                    join_clause = f"JOIN {business_table} ON {' AND '.join(join_conditions)}"
                    join_clauses.append(join_clause)
            
            if join_clauses:
                new_from += " " + " ".join(join_clauses)
            
            new_sql = from_pattern.sub(new_from, sql)
        
        # 5. 为可能产生歧义的列添加表名前缀
        for col, tables in column_to_table.items():
            if len(tables) > 1 and col in new_sql:
                # 这个列在多个表中存在，需要确定使用哪个表
                # 优先选择在needed_business_tables中且包含该列的表
                available_tables = [t for t in tables if t in needed_business_tables]
                if available_tables:
                    table_for_col = available_tables[0]
                    # 使用更精确的替换
                    col_pattern = re.compile(r'(?<!\w\.)\b' + re.escape(col) + r'\b(?!\s*\.)')
                    new_sql = col_pattern.sub(f"{table_for_col}.{col}", new_sql)
        
        # 6. 处理表别名
        # 如果原SQL使用了表别名，需要相应调整
        alias_pattern = re.compile(r'\b' + re.escape(self.old_table) + r'\s+(\w+)', re.IGNORECASE)
        alias_match = alias_pattern.search(sql)
        if alias_match:
            old_alias = alias_match.group(1)
            # 在改写后的SQL中替换别名引用
            new_sql = re.sub(r'\b' + re.escape(old_alias) + r'\.', '', new_sql)
        
        return new_sql
    

    def apply_to_data(self, data_dict):
        if pd is None:
            raise ImportError("需要安装 pandas 才能执行数据级别的数据拆分 (apply_to_data)")
        result = data_dict.copy()
        
        if self.old_table not in result:
            raise ValueError(f"[TableSplit] 原表 {self.old_table} 不存在")
        
        old_df = result[self.old_table]
        original_columns = set(old_df.columns)
        
        # 第一步：识别所有唯一的主键列（去重）
        all_primary_keys = set()
        for pk_columns in self.primary_keys_dict.values():
            all_primary_keys.update(pk_columns)
        
        # 验证所有主键列都存在
        for pk_col in all_primary_keys:
            if pk_col not in old_df.columns:
                raise ValueError(f"[TableSplit] 主键列 {pk_col} 在原表 {self.old_table} 中不存在")
        
        # 第二步：创建独立的主键表（包含所有唯一的主键列组合）
        pk_table_name = f"{self.old_table}_keys"
        if pk_table_name in result:
            raise ValueError(f"[TableSplit] 主键表名 {pk_table_name} 已存在")
        
        # 创建主键表（去除重复的主键组合）
        pk_df = old_df[list(all_primary_keys)].drop_duplicates().reset_index(drop=True)
        result[pk_table_name] = pk_df
        print(f"[TableSplit] 创建独立主键表: {pk_table_name}，包含列: {list(all_primary_keys)}")
        print(f"主键表数据 ({len(pk_df)} 行):")
        print(pk_df)
        
        # 第三步：为每个新表创建业务表（去除完全重复的行）
        migrated_columns = set()
        
        for i, new_table in enumerate(self.new_tables):
            if new_table in result:
                raise ValueError(f"[TableSplit] 新表名 {new_table} 已存在")
            
            # 获取该新表的主键列（根据主键字典）
            table_primary_keys = self.primary_keys_dict.get(new_table, [])
            
            # 获取该新表的业务列
            business_columns = []
            if i < len(self.columnList):
                business_columns = self.columnList[i]
            
            # 构建新表的所有列：主键部分 + 业务列
            all_table_columns = table_primary_keys + business_columns
            
            # 验证列是否存在
            for col in all_table_columns:
                if col not in old_df.columns:
                    raise ValueError(f"[TableSplit] 列 {col} 在原表 {self.old_table} 中不存在")
            
            # 创建新表并去除完全重复的行
            new_df = old_df[all_table_columns].drop_duplicates().reset_index(drop=True)
            result[new_table] = new_df
            
            # 记录已迁移的列
            migrated_columns.update(all_table_columns)
            
            print(f"\n[TableSplit] 创建业务表: {new_table}")
            print(f"  - 主键部分: {table_primary_keys}")
            print(f"  - 业务列: {business_columns}")
            print(f"  - 总列数: {len(all_table_columns)}")
            print(f"  - 去重后行数: {len(new_df)} 行")
            print(f"  - 数据预览:")
            print(new_df)
        
        # 第四步：检查所有列是否都已迁移
        remaining_columns = original_columns - migrated_columns
        if remaining_columns:
            raise ValueError(
                f"[TableSplit] 源表 {self.old_table} 仍有列未被迁移: {sorted(remaining_columns)}"
            )
        
        # 第五步：验证无损分解
        self._verify_lossless_decomposition(result, old_df, pk_table_name)
        
        # 第六步：记录表关系映射
        table_mapping = {
            'primary_key_table': pk_table_name,
            'business_tables': {
                table: {
                    'primary_keys': self.primary_keys_dict.get(table, []),
                    'business_columns': self.columnList[i] if i < len(self.columnList) else [],
                    'row_count': len(result[table])
                }
                for i, table in enumerate(self.new_tables)
            },
            'all_primary_keys': list(all_primary_keys),
            'original_row_count': len(old_df)
        }
        result[f"{self.old_table}_mapping"] = table_mapping
        
        # 移除原表
        result.pop(self.old_table, None)
        
        print(f"\n[TableSplit] 表拆分完成！")
        print(f"  - 原表: {self.old_table} ({len(old_df)} 行)")
        print(f"  - 主键表: {pk_table_name} ({len(pk_df)} 行)")
        for table_name in self.new_tables:
            print(f"  - {table_name}: ({len(result[table_name])} 行)")
        
        return result

    def _verify_lossless_decomposition(self, result, original_df, pk_table_name):
        """
        验证无损分解：确保通过JOIN可以还原原始数据
        """
        print(f"\n[TableSplit] 验证无损分解...")
        
        try:
            # 获取所有表
            pk_df = result[pk_table_name]
            
            # 构建JOIN查询来还原数据
            reconstructed_df = pk_df.copy()
            
            # 按正确的顺序JOIN所有业务表
            for table_name in self.new_tables:
                if table_name in result:
                    business_df = result[table_name]
                    
                    # 找出共同的主键列进行JOIN
                    common_columns = list(set(reconstructed_df.columns) & set(business_df.columns))
                    if common_columns:
                        # 使用left join确保不丢失主键表的任何行
                        reconstructed_df = pd.merge(reconstructed_df, business_df, on=common_columns, how='left')
                        print(f"  ✓ 已JOIN表 {table_name}，关联列: {common_columns}")
            
            # 按原始列顺序重新排列
            reconstructed_df = reconstructed_df[original_df.columns]
            
            # 排序后比较
            sort_columns = list(pk_df.columns)
            reconstructed_sorted = reconstructed_df.sort_values(by=sort_columns).reset_index(drop=True)
            original_sorted = original_df.sort_values(by=sort_columns).reset_index(drop=True)
            
            print(f"\n数据对比:")
            print(f"  原始数据: {len(original_sorted)} 行")
            print(original_sorted)
            print(f"  还原数据: {len(reconstructed_sorted)} 行")
            print(reconstructed_sorted)
            
            # 检查数据一致性
            if reconstructed_sorted.equals(original_sorted):
                print("  ✓ 无损分解验证通过：数据可以完全还原")
            else:
                print("  ✗ 无损分解验证失败：还原数据与原始数据不一致")
                
                # 显示具体差异
                print(f"\n差异分析:")
                for idx in range(min(len(original_sorted), len(reconstructed_sorted))):
                    if not reconstructed_sorted.iloc[idx].equals(original_sorted.iloc[idx]):
                        print(f"  第{idx}行不一致:")
                        print(f"    原始: {original_sorted.iloc[idx].to_dict()}")
                        print(f"    还原: {reconstructed_sorted.iloc[idx].to_dict()}")
                        break
                
        except Exception as e:
            print(f"  ✗ 无损分解验证失败: {e}")
            import traceback
            traceback.print_exc()
            
        



def test_student_course_decomposition():
    """测试学生选课表的无损分解"""
    
    # 创建学生选课表测试数据
    input_data = {
        'student_courses': pd.DataFrame({
            'student_id': [1, 1, 1, 2, 2, 3, 3, 4],
            'student_name': ['张三', '张三', '张三', '李四', '李四', '王五', '王五', '赵六'],
            'student_major': ['计算机', '计算机', '计算机', '数学', '数学', '物理', '物理', '化学'],
            'course_id': [101, 102, 103, 101, 104, 102, 105, 101],
            'course_name': ['数据库', '算法', '网络', '数据库', '统计学', '算法', '量子力学', '数据库'],
            'credit': [3, 4, 3, 3, 4, 4, 3, 3],
            'grade': [85, 92, 78, 88, 95, 90, 85, 82],
            'semester': ['2023春', '2023春', '2023秋', '2023春', '2023秋', '2023春', '2023秋', '2023春']
        })
    }
    
    print("原始学生选课表:")
    print(input_data['student_courses'])
    print(f"\n原始表列: {list(input_data['student_courses'].columns)}")
    
    # 定义拆分规则 - 将学生选课表拆分为三个表
    split_operation = TableSplit(
        old_table='student_courses',
        new_tables=['students', 'courses', 'enrollments'],
        columnList=[
            ['student_name', 'student_major'],    # students 表的业务列
            ['course_name', 'credit'],            # courses 表的业务列
            ['grade', 'semester']                 # enrollments 表的业务列
        ],
        primary_keys_dict={
            'students': ['student_id'],           # 学生表主键
            'courses': ['course_id'],             # 课程表主键
            'enrollments': ['student_id', 'course_id']  # 选课表复合主键
        }
    )
    
    # 执行拆分
    output_data = split_operation.apply_to_data(input_data.copy())
    
    return input_data, output_data, split_operation

def verify_student_course_reconstruction(input_data, output_data):
    """验证学生选课表可以通过JOIN还原"""
    
    print(f"\n{'='*60}")
    print("验证无损分解 - 通过JOIN还原原始数据")
    print(f"{'='*60}")
    
    # 获取拆分后的各个表
    pk_table = output_data['student_courses_keys']
    students_table = output_data['students']
    courses_table = output_data['courses']
    enrollments_table = output_data['enrollments']
    
    print(f"\n拆分后的表结构:")
    print(f"主键表 ({pk_table.shape}): {list(pk_table.columns)}")
    print(f"学生表 ({students_table.shape}): {list(students_table.columns)}")
    print(f"课程表 ({courses_table.shape}): {list(courses_table.columns)}")
    print(f"选课表 ({enrollments_table.shape}): {list(enrollments_table.columns)}")
    
    # 显示各表数据
    print(f"\n主键表数据:")
    print(pk_table)
    
    print(f"\n学生表数据:")
    print(students_table)
    
    print(f"\n课程表数据:")
    print(courses_table)
    
    print(f"\n选课表数据:")
    print(enrollments_table)
    
    # 通过JOIN操作还原原始数据
    print(f"\n通过JOIN还原数据...")
    
    # 第一步：主键表 + 学生表
    step1 = pd.merge(pk_table, students_table, on=['student_id'], how='left')
    print(f"✓ 主键表 JOIN 学生表: {step1.shape}")
    print(step1)
    
    # 第二步：加入课程表
    step2 = pd.merge(step1, courses_table, on=['course_id'], how='left')
    print(f"✓ 加入课程表: {step2.shape}")
    print(step2)
    
    # 第三步：加入选课表
    reconstructed = pd.merge(step2, enrollments_table, on=['student_id', 'course_id'], how='left')
    print(f"✓ 加入选课表: {reconstructed.shape}")
    
    # 按原始列顺序排列
    original_columns = input_data['student_courses'].columns
    reconstructed = reconstructed[original_columns]
    
    # 排序后比较
    original_sorted = input_data['student_courses'].sort_values(['student_id', 'course_id']).reset_index(drop=True)
    reconstructed_sorted = reconstructed.sort_values(['student_id', 'course_id']).reset_index(drop=True)
    
    print(f"\n还原验证结果:")
    print(f"原始数据形状: {original_sorted.shape}")
    print(f"还原数据形状: {reconstructed_sorted.shape}")
    print(f"数据完全一致: {original_sorted.equals(reconstructed_sorted)}")
    
    if not original_sorted.equals(reconstructed_sorted):
        print(f"\n数据差异分析:")
        print("原始数据:")
        print(original_sorted)
        print("\n还原数据:")
        print(reconstructed_sorted)
    
    return reconstructed_sorted

def demonstrate_sql_rewriting_with_pk_table(input_data, output_data, split_operation):
    """使用主键表的SQL改写功能测试"""
    
    print(f"\n{'='*60}")
    print("SQL改写功能测试 (使用主键表)")
    print(f"{'='*60}")
    
    # 主键表名
    pk_table_name = f"{split_operation.old_table}_keys"
    
    # 测试用例
    test_sqls = [
        {
            "description": "1. 简单SELECT * 查询",
            "sql": "SELECT * FROM student_courses",
            "expected_tables": [pk_table_name, "students", "courses", "enrollments"],
            "should_have_join": True,
            "should_expand_select": True
        },
        {
            "description": "2. 纯学生表查询",
            "sql": "SELECT student_id, student_name FROM student_courses WHERE student_major = '计算机'",
            "expected_tables": ["students"],  # 只需要学生表
            "should_have_join": False,
            "should_expand_select": False
        },
        {
            "description": "3. 纯课程表查询", 
            "sql": "SELECT course_id, course_name, credit FROM student_courses WHERE credit > 3",
            "expected_tables": ["courses"],  # 只需要课程表
            "should_have_join": False,
            "should_expand_select": False
        },
        {
            "description": "4. 纯选课表查询",
            "sql": "SELECT student_id, course_id, grade FROM student_courses WHERE grade > 90",
            "expected_tables": ["enrollments"],  # 只需要选课表
            "should_have_join": False,
            "should_expand_select": False
        },
        {
            "description": "5. 跨表查询需要JOIN",
            "sql": "SELECT student_name, course_name, grade, semester FROM student_courses",
            "expected_tables": [pk_table_name, "students", "courses", "enrollments"],
            "should_have_join": True,
            "should_expand_select": False
        },
        {
            "description": "6. 学生和成绩联合查询",
            "sql": "SELECT student_name, grade FROM student_courses",
            "expected_tables": [pk_table_name, "students", "enrollments"],
            "should_have_join": True,
            "should_expand_select": False
        },
        {
            "description": "7. 课程和成绩联合查询",
            "sql": "SELECT course_name, grade FROM student_courses",
            "expected_tables": [pk_table_name, "courses", "enrollments"],
            "should_have_join": True,
            "should_expand_select": False
        }
    ]
    
    print(f"原表: {split_operation.old_table}")
    print(f"主键表: {pk_table_name}")
    print(f"业务表: {split_operation.new_tables}")
    
    for test_case in test_sqls:
        print(f"\n{test_case['description']}")
        print(f"  原SQL: {test_case['sql']}")
        
        try:
            # 应用SQL改写
            rewritten_sql = split_operation.apply_to_sql(test_case['sql'])
            print(f"  改写后SQL: {rewritten_sql}")
            
            # 分析改写结果
            analyze_rewritten_with_pk_table(rewritten_sql, test_case, split_operation, pk_table_name)
            
        except Exception as e:
            print(f"  ✗ SQL改写失败: {e}")

def analyze_rewritten_with_pk_table(rewritten_sql, test_case, split_operation, pk_table_name):
    """使用主键表的SQL改写分析"""
    
    print(f"  改写分析:")
    
    # 1. 基本语法检查
    issues = []
    
    # 检查是否还包含原表名
    if split_operation.old_table.lower() in rewritten_sql.lower():
        issues.append("✗ 仍然包含原表名")
    
    # 2. 表使用检查
    missing_tables = []
    for table in test_case['expected_tables']:
        if table.lower() not in rewritten_sql.lower():
            missing_tables.append(table)
    
    if missing_tables:
        issues.append(f"✗ 缺少表: {missing_tables}")
    
    # 检查不需要的表
    unexpected_tables = []
    all_possible_tables = [pk_table_name] + split_operation.new_tables
    for table in all_possible_tables:
        if (table.lower() in rewritten_sql.lower() and 
            table not in test_case['expected_tables']):
            unexpected_tables.append(table)
    
    if unexpected_tables:
        issues.append(f"⚠ 包含不需要的表: {unexpected_tables}")
    
    # 3. JOIN检查
    has_join = any(keyword in rewritten_sql.upper() for keyword in [" JOIN ", " INNER JOIN ", " LEFT JOIN "])
    if test_case['should_have_join'] and not has_join:
        issues.append("✗ 应该包含JOIN操作但缺少")
    elif not test_case['should_have_join'] and has_join:
        issues.append("⚠ 不需要JOIN但包含了JOIN操作")
    
    # 4. SELECT检查
    if test_case['should_expand_select'] and "SELECT *" in rewritten_sql.upper():
        issues.append("✗ SELECT * 没有被展开")
    
    # 5. 输出结果
    if issues:
        for issue in issues:
            print(f"    {issue}")
    else:
        print(f"    ✓ SQL改写正确")
        
        # 显示成功细节
        details = []
        details.append("原表名已替换")
        if pk_table_name in test_case['expected_tables']:
            details.append("使用主键表连接")
        if test_case['should_have_join']:
            details.append("JOIN操作正确")
        if test_case['should_expand_select']:
            details.append("SELECT * 已展开")
        
        print(f"    → {', '.join(details)}")

# 运行测试
if __name__ == "__main__":
    # 测试学生选课表分解
    input_data, output_data, split_operation = test_student_course_decomposition()
    
    # 验证无损分解
    reconstructed = verify_student_course_reconstruction(input_data, output_data)
    
    # 测试SQL改写功能
    demonstrate_sql_rewriting_with_pk_table(input_data, output_data, split_operation)
    
    
    
    # 显示表关系映射
    print(f"\n{'='*60}")
    print("表关系映射信息")
    print(f"{'='*60}")
    mapping = output_data['student_courses_mapping']
    print(f"主键表: {mapping['primary_key_table']}")
    print(f"所有主键列: {mapping['all_primary_keys']}")
    print(f"\n业务表详情:")
    for table_name, table_info in mapping['business_tables'].items():
        print(f"  {table_name}:")
        print(f"    主键: {table_info['primary_keys']}")
        print(f"    业务列: {table_info['business_columns']}")
