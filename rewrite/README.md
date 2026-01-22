# rewrite各个功能调用标准写法示例


创建公用数据库连接  
from DataBase.MySQLDriver import MySQLDriver  
db = MySQLDriver({'user':'root','password':'123！@#200','database':'tpcch'}); db.connect()  

### 列重命名  
创建类实例  
```python  
from rewrite.ColumnRename import ColumnRename  

#表名，旧列名，新列名
op = ColumnRename('orders','o_carrier_id','carrier_id')
```

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作  
```python
new_sql = op.apply_to_sql(sql)  
print(new_sql)
```  

### 列拆分  
创建类实例  
```python 
from rewrite.ColumnSplit import ColumnSplit  
#表名，要拆分的列，拆分后列名列表，分隔符或者拆分pos位置
op = ColumnSplit('orderline','ol_dist_info',['dist_a','dist_b'], split_delimiter='-')
```

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作  
```python
new_sql = op.apply_to_sql(sql)  
print(new_sql) 
```  

### 表垂直合并TableJoin  
创建类实例  
```python
from rewrite.TableJoin import TableJoin

orderline_cols = ['ol_o_id','ol_d_id','ol_w_id','ol_number','ol_i_id','ol_supply_w_id','ol_delivery_d','ol_quantity','ol_amount','ol_dist_info']

orders_cols = ['o_id','o_d_id','o_w_id','o_c_id','o_entry_d','o_carrier_id','o_ol_cnt','o_all_local']
# 两个旧表名列表，连接后新表名，旧表名列表[[table1_columns], [table2_columns]]，是否保留旧表标志（sign=1不保留），连接条件（可以多个）
op = TableJoin(['orderline','orders'],'orderline_orders',[orderline_cols,orders_cols], sign=2, join_key=[('ol_w_id','o_w_id'),('ol_d_id','o_d_id'),('ol_o_id','o_id')])
```

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作，需要提供新表名
```python 
#需要根据是否保留原表选择改写策略
new_sql = op._replace_strategy1(sql, new_table_name)
new_sql = op._replace_strategy2(sql, new_table_name)
print(new_sql) 
```  

### 表垂直拆分TableSplit  
创建新实例  
```python
from rewrite.TableSplit import TableSplit  
# 旧表名，拆分成的新表名列表，新表中对应的列（字典形式），新表中主键列（字典），视图名（可选），是否保留原表标志
op = TableSplit('orderline', ['orderline_main','orderline_info'], {'orderline_main':['ol_w_id','ol_d_id','ol_o_id','ol_number','ol_i_id','ol_quantity','ol_amount','ol_delivery_d'], 'orderline_info':['ol_w_id','ol_d_id','ol_o_id','ol_number','ol_dist_info']}, {'orderline_main':['ol_w_id','ol_d_id','ol_o_id','ol_number'], 'orderline_info':['ol_w_id','ol_d_id','ol_o_id','ol_number']}, new_view='view_orderline',is_retained = )
``` 

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作，需要提供新表名
```python 
#需要根据是否保留原表选择改写策略
new_sql = op._replace_strategy1(sql, new_table_name)
new_sql = op._replace_strategy2(sql, new_table_name)
print(new_sql) 
```  

### 表水平合并HorizontalMerge  
创建新实例  
```python
from rewrite.HorizontalMerge import HorizontalMerge  
# 源表列表，合并后的新表名，是否保留原表标志
op = HorizontalMerge(['orderline_early','orderline_late'], 'orderline_all_view', is_retained=True)
```

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作，需要提供新表名
```python 
#需要根据是否保留原表选择改写策略
new_sql = op.apply_to_sql(sql)
print(new_sql) 
```  

### 表水平拆分HorizontalSplit  
创建新实例  
```python 
from rewrite.HorizontalSplit import HorizontalSplit
# 原表名，列表[新表名+携带的拆分依据]，是否保留原表标志
op = HorizontalSplit('orderline', [('orderline_early',"ol_delivery_d<'2012-01-01'"),('orderline_late',"ol_delivery_d>='2012-01-01'")], is_retained=False)
```

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作，需要提供新表名
```python 
#需要根据是否保留原表选择改写策略
new_sql = op.apply_to_sql(sql)
print(new_sql) 
```  

### 增加冗余列  
创建新实例  
```python 
from rewrite.RedundantColumnAdd import RedundantColumnAdd  
# 取出冗余列的表，列名，要冗余操作的表名，冗余后的列名，连接条件列表
op = RedundantColumnAdd('item','i_name','stock','i_name', join_keys=[('i_id','s_i_id')])
```

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作，需要提供新表名
```python 
#需要根据是否保留原表选择改写策略
new_sql = op.apply_to_sql(sql)
print(new_sql) 
```  

### 删除冗余列
创建新实例
```python
from rewrite.RedundantColumnDrop import RedundantColumnDrop
# 删除冗余列的表，冗余列名，冗余列来源表，来源表中的列名，连接条件
op = RedundantColumnDrop('stock','i_name','item','i_name', join_keys=[('i_id','s_i_id')])
```

执行schema操作  
`op.apply_to_schema(db)`  

执行SQL操作，需要提供新表名
```python 
#需要根据是否保留原表选择改写策略
new_sql = op.apply_to_sql(sql)
print(new_sql) 
```  
