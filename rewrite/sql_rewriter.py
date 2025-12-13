import sqlglot
from sqlglot import expressions as exp
from typing import Optional, List, Tuple

def rewrite_join_to_view(
    sql: str,
    old_tables: List[str],
    new_view: str
) -> str:
    """
    将 SQL 中 JOIN 的两个表替换为单个视图
    
    Args:
        sql: 原始 SQL 语句
        old_tables: 需要被替换的旧表名列表（必须正好2个）
        new_view: 替换后的新视图名
    
    Returns:
        改写后的 SQL 语句
    """
    # 参数验证
    if len(old_tables) != 2:
        return sql
    
    try:
        # 解析 SQL
        tree = sqlglot.parse_one(sql)
        
        # 只处理 SELECT 语句
        if not isinstance(tree, exp.Select):
            return sql
        
        # 查找 FROM 表
        from_table = _get_table_name(tree.args.get("from"))
        
        # 查找 JOIN 表
        joins = tree.args.get("joins", [])
        for join in joins:
            join_table = _get_table_name(join.this)
            
            # 检查是否是我们要替换的表对
            if (join_table in old_tables and from_table in old_tables and
                join_table != from_table):
                
                # 替换 FROM 表为新的视图
                _replace_table(tree, new_view)
                
                # 移除这个 JOIN
                joins.remove(join)
                
                # 只处理第一个匹配的 JOIN
                break
        
        return tree.sql()
        
    except Exception:
        # 解析失败或处理出错时返回原 SQL
        return sql


def _get_table_name(node) -> Optional[str]:
    """从节点中提取表名"""
    if not node:
        return None
    
    # 表节点
    if isinstance(node, exp.Table):
        return node.name
    
    # 带别名的表
    if isinstance(node, exp.Alias):
        inner = node.this
        if isinstance(inner, exp.Table):
            return inner.name
    
    return None


def _replace_table(select_node: exp.Select, new_view: str):
    """替换 SELECT 的 FROM 表"""
    from_node = select_node.args.get("from")
    if not from_node:
        return
    
    # 创建新表
    new_table = exp.Table(
        this=exp.Identifier(this=new_view, quoted=False)
    )
    
    # 保持原有别名
    if isinstance(from_node, exp.Alias):
        select_node.args["from"] = exp.Alias(
            this=new_table,
            alias=from_node.alias
        )
    else:
        select_node.args["from"] = new_table


def rewrite_join_pairs(
    sql: str,
    table_pairs: List[Tuple[Tuple[str, str], str]]
) -> str:
    """
    批量替换多个 JOIN 表对
    
    Args:
        sql: 原始 SQL 语句
        table_pairs: [((table1, table2), new_view), ...]
    
    Returns:
        改写后的 SQL 语句
    """
    try:
        tree = sqlglot.parse_one(sql)
        if not isinstance(tree, exp.Select):
            return sql
        
        # 多次应用替换（最多10次避免无限循环）
        for _ in range(10):
            changed = False
            from_table = _get_table_name(tree.args.get("from"))
            
            joins = tree.args.get("joins", [])
            for join in joins:
                join_table = _get_table_name(join.this)
                
                if from_table and join_table:
                    # 查找匹配的替换规则
                    for (t1, t2), new_view in table_pairs:
                        if {from_table, join_table} == {t1, t2}:
                            # 替换表
                            _replace_table(tree, new_view)
                            joins.remove(join)
                            changed = True
                            break
                
                if changed:
                    break
            
            if not changed:
                break
        
        return tree.sql()
        
    except Exception:
        return sql
    

