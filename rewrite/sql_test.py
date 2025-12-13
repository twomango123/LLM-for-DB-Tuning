import sql_rewriter as sr

def test_simple_join():
    """测试简单的 JOIN 替换"""
    print("=== 测试1: 简单 JOIN 替换 ===")
    
    sql = """
    SELECT o.id, c.name 
    FROM orders o 
    LEFT JOIN customers c ON o.customer_id = c.id
    """
    
    result = sr.rewrite_join_to_view(
        sql=sql,
        old_tables=["orders", "customers"],
        new_view="orders_customers_view"
    )
    
    print("原SQL:")
    print(sql.strip())
    print("\n结果:")
    print(result.strip())
    print()

def test_no_match():
    """测试不匹配的情况"""
    print("=== 测试2: 不匹配的表 ===")
    
    sql = """
    SELECT o.id, p.name 
    FROM orders o 
    LEFT JOIN products p ON o.product_id = p.id
    """
    
    result = sr.rewrite_join_to_view(
        sql=sql,
        old_tables=["orders", "customers"],  # 这里是 customers，不是 products
        new_view="orders_customers_view"
    )
    
    print("原SQL:")
    print(sql.strip())
    print("\n结果（应该不变）:")
    print(result.strip())
    print()

def test_with_where():
    """测试带 WHERE 子句的 JOIN"""
    print("=== 测试3: 带 WHERE 子句 ===")
    
    sql = """
    SELECT o.id, c.name, o.amount
    FROM orders o 
    INNER JOIN customers c ON o.customer_id = c.id
    WHERE o.status = 'active' 
      AND c.country = 'US'
    ORDER BY o.amount DESC
    """
    
    result = sr.rewrite_join_to_view(
        sql=sql,
        old_tables=["orders", "customers"],
        new_view="orders_customers_view"
    )
    
    print("原SQL:")
    print(sql.strip())
    print("\n结果:")
    print(result.strip())
    print()

def test_multiple_joins():
    """测试多个 JOIN"""
    print("=== 测试4: 多个 JOIN ===")
    
    sql = """
    SELECT o.id, c.name, p.name
    FROM orders o 
    LEFT JOIN customers c ON o.customer_id = c.id
    LEFT JOIN products p ON o.product_id = p.id
    """
    
    result = sr.rewrite_join_to_view(
        sql=sql,
        old_tables=["orders", "customers"],
        new_view="orders_customers_view"
    )
    
    print("原SQL:")
    print(sql.strip())
    print("\n结果:")
    print(result.strip())
    print()

def test_batch_rewrite():
    """测试批量替换多个 JOIN 对"""
    print("=== 测试5: 批量替换 ===")
    
    sql = """
    SELECT o.id, c.name, p.name, cat.name
    FROM orders o 
    LEFT JOIN customers c ON o.customer_id = c.id
    LEFT JOIN products p ON o.product_id = p.id
    LEFT JOIN categories cat ON p.category_id = cat.id
    """
    
    # 定义多个替换规则
    table_pairs = [
        (("orders", "customers"), "orders_customers_view"),
        (("products", "categories"), "product_category_view")
    ]
    
    result = sr.rewrite_join_pairs(sql, table_pairs)
    
    print("原SQL:")
    print(sql.strip())
    print("\n结果:")
    print(result.strip())
    print()

def test_edge_cases():
    """测试边界情况"""
    print("=== 测试6: 边界情况 ===")
    
    # 1. 非 SELECT 语句
    insert_sql = "INSERT INTO orders (id, name) VALUES (1, 'test')"
    result = sr.rewrite_join_to_view(
        insert_sql,
        old_tables=["orders", "customers"],
        new_view="orders_customers_view"
    )
    print("1. 非 SELECT 语句（应该不变）:")
    print(f"输入: {insert_sql}")
    print(f"输出: {result}")
    print()
    
    # 2. 无效 SQL
    invalid_sql = "SELECT * FROM"
    result = sr.rewrite_join_to_view(
        invalid_sql,
        old_tables=["orders", "customers"],
        new_view="orders_customers_view"
    )
    print("2. 无效 SQL（应该返回原SQL）:")
    print(f"输入: {invalid_sql}")
    print(f"输出: {result}")
    print()
    
    # 3. 只有一个表
    single_table_sql = "SELECT * FROM orders"
    result = sr.rewrite_join_to_view(
        single_table_sql,
        old_tables=["orders", "customers"],
        new_view="orders_customers_view"
    )
    print("3. 只有一个表（应该不变）:")
    print(f"输入: {single_table_sql}")
    print(f"输出: {result}")

def test_with_subquery():
    """测试包含子查询的情况"""
    print("=== 测试7: 包含子查询 ===")
    
    sql = """
    SELECT * FROM (
        SELECT o.id, c.name 
        FROM orders o 
        JOIN customers c ON o.customer_id = c.id
    ) AS subquery
    """
    
    result = sr.rewrite_join_to_view(
        sql=sql,
        old_tables=["orders", "customers"],
        new_view="orders_customers_view"
    )
    
    print("原SQL:")
    print(sql.strip())
    print("\n结果:")
    print(result.strip())

if __name__ == "__main__":
    # 运行所有测试
    test_simple_join()
    test_no_match()
    test_with_where()
    test_multiple_joins()
    test_batch_rewrite()
    test_edge_cases()
    test_with_subquery()