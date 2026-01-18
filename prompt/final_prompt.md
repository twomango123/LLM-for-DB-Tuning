背景：

你是一个数据库性能调优专家，需要进行数据库模式修改以提高系统的性能表现(降低查询延迟)。

信息：

数据库当前的模式为：

"products": {
	"product_id": "INT",
	"product_name": "VARCHAR",
	"product_price": "DECIMAL",
	"product_description": "VARCHAR"
},
"addresses": {
	"address_id": "INT",
	"address_details": "VARCHAR",
	"city": "VARCHAR",
	"zip_postcode": "VARCHAR",
	"state_province_county": "VARCHAR",
	"country": "VARCHAR"
},
"customers": {
	"customer_id": "INT",
	"payment_method": "VARCHAR",
	"customer_name": "VARCHAR",
	"customer_phone": "VARCHAR",
	"customer_email": "VARCHAR",
	"date_became_customer": "DATETIME"
},
"regular_orders": {
	"regular_order_id": "INT",
	"distributer_id": "INT"
},
"regular_order_products": {
	"regular_order_id": "INT",
	"product_id": "INT"
},
"actual_orders": {
	"actual_order_id": "INT",
	"order_status_code": "VARCHAR",
	"regular_order_id": "INT",
	"actual_order_date": "DATETIME"
},
"actual_order_products": {
	"actual_order_id": "INT",
	"product_id": "INT"
},
"customer_addresses": {
	"customer_id": "INT",
	"address_id": "INT",
	"date_from": "DATETIME",
	"address_type": "VARCHAR",
	"date_to": "DATETIME"
},
"delivery_routes": {
	"route_id": "INT",
	"route_name": "VARCHAR",
	"other_route_details": "VARCHAR"
},
"delivery_route_locations": {
	"location_code": "VARCHAR",
	"route_id": "INT",
	"location_address_id": "INT",
	"location_name": "VARCHAR"
},
"trucks": {
	"truck_id": "INT",
	"truck_licence_number": "VARCHAR",
	"truck_details": "VARCHAR"
},
"employees": {
	"employee_id": "INT",
	"employee_address_id": "INT",
	"employee_name": "VARCHAR",
	"employee_phone": "VARCHAR"
},
"order_deliveries": {
	"location_code": "VARCHAR",
	"actual_order_id": "INT",
	"delivery_status_code": "VARCHAR",
	"driver_employee_id": "INT",
	"truck_id": "INT",
	"delivery_date": "DATETIME"
}

列级操作与基数统计：

{
  "trucks": {
    "truck_licence_number": [
      {
        "operation": "order by",
        "rows": 1
      }
    ]
  },
  "products": {
    "product_price": [
      {
        "operation": "order by",
        "rows": 1
      }
    ]
  },
  "customers": {
    "date_became_customer": [
      {
        "operation": "order by",
        "rows": 1
      }
    ],
    "payment_method": [
      {
        "operation": "group by",
        "rows": 1
      }
    ]
  },
  "delivery_routes": {
    "route_name": [
      {
        "operation": "order by",
        "rows": 1
      }
    ]
  }
}

历史负载及其在当前模式下的执行时间为：

~~~sql
-- SQL1 : N/A ms --
SELECT actual_order_id FROM actual_orders WHERE order_status_code  =  'Success';


-- SQL2 : N/A ms --
SELECT t1.product_name ,  t1.product_price FROM products AS t1 JOIN regular_order_products AS t2 ON t1.product_id  =  t2.product_id GROUP BY t2.product_id ORDER BY count(*) DESC LIMIT 1;


-- SQL3 : N/A ms --
SELECT count(*) FROM customers;


-- SQL4 : N/A ms --
SELECT count(DISTINCT payment_method) FROM customers;


-- SQL5 : N/A ms --
SELECT truck_details FROM trucks ORDER BY truck_licence_number;


-- SQL6 : N/A ms --
SELECT product_name FROM products ORDER BY product_price DESC LIMIT 1;


-- SQL7 : N/A ms --
SELECT DISTINCT c.customer_name
FROM customers AS c
WHERE NOT EXISTS (
  SELECT 1
  FROM customer_addresses AS ca
  JOIN addresses AS a ON ca.address_id = a.address_id
  WHERE ca.customer_id = c.customer_id
    AND a.state_province_county = 'California'
);


-- SQL8 : N/A ms --
SELECT customer_email ,  customer_name FROM customers WHERE payment_method  =  'Visa';


-- SQL9 : N/A ms --
SELECT t1.customer_name ,  t1.customer_phone FROM customers AS t1 JOIN customer_addresses AS t2 ON t1.customer_id  =  t2.customer_id JOIN addresses AS t3 ON t2.address_id  =  t3.address_id WHERE t3.state_province_county  =  'California';


-- SQL10 : N/A ms --
SELECT state_province_county FROM addresses WHERE address_id NOT IN (SELECT employee_address_id FROM employees);


-- SQL11 : N/A ms --
SELECT customer_name ,  customer_phone ,  customer_email FROM customers ORDER BY date_became_customer;


-- SQL12 : N/A ms --
SELECT customer_name FROM customers ORDER BY date_became_customer LIMIT 5;


-- SQL13 : N/A ms --
SELECT payment_method FROM customers GROUP BY payment_method ORDER BY count(*) DESC LIMIT 1;


-- SQL14 : N/A ms --
SELECT route_name FROM delivery_routes ORDER BY route_name;


-- SQL15 : N/A ms --
SELECT t1.route_name FROM delivery_routes AS t1 JOIN delivery_route_locations AS t2 ON t1.route_id  =  t2.route_id GROUP BY t1.route_id ORDER BY count(*) DESC LIMIT 1;


-- SQL16 : N/A ms --
SELECT t2.state_province_county ,  count(*) FROM customer_addresses AS t1 JOIN addresses AS t2 ON t1.address_id  =  t2.address_id GROUP BY t2.state_province_county;
~~~

## 操作集合  
{
	"ColumnSplit": {
	"操作含义": "将一个属性拆分为多个子属性，可选保留或删除原属性",
	"接口": "ColumnSplit(SourceTable.Column, is_retained):NewCol1(表达式/规则),NewCol2(表达式/规则)[,...]",
	"举例": "ColumnSplit(users.email, True):email_user(split('@',1)),email_domain(split('@',2))",
	"约束条件": "不允许对自增/唯一/检查的约束列执行该操作"
	},

	"VerticalSplit": {
	"操作含义": "按列将一张表垂直拆分为多个子表，每个子表保留原主键列，可选保留或删除原表",
	"接口": "VerticalSplit(SourceTable, is_retained):table1(attribute1, ...),table2(attribute2, ...), table1(primary_key...), table2(primary_key...)",
	"举例": "VerticalSplit(CUSTOMER, True):C1(c_id,c_name,c_sex),C2(c_id,c_birthday,c_level), C1(c_id), C2(c_id)",
	"约束条件": "（不保留原表）每个子表必须包含全部主键列；同一外键的组成列不得拆到不同子表"
	},
	"TableJoin": {
	"操作含义": "将两个表通过连接条件合并为一个表，可选保留或删除原表",
	"接口": "TableJoin(Table1,Table2, table1_join_key, table2_join_key, is_retained): NewTable",
	"举例": "TableJoin(customer,customer_ext, customer):customer_all",
	},

	"HorizontalSplit": {
	"操作含义": "按谓词将表水平拆分成多个分表，可选保留或删除原表",
	"接口": "HorizontalSplit(SourceTable):Table1(拆分依据),Table2(拆分依据),....",
	"举例": "HorizontalSplit(orders):orders_2023(year=2023), orders_2024(year=2024)",
	"约束条件": "当原表不保留，且表主键是其他表的外键时，允许操作，但操作会使其他表丢失外键约束。"
	},

	"HorizontalMerge": {
	"操作含义": "将同结构子表，水平合并为新表，可选保留或删除原表",
	"接口": "HorizontalMerge(Table1, Table2, is_remained):NewTable",
	"举例": "HorizontalMerge(orders_2023, orders_2024, False):orders_all",
	"约束条件": "两子表需具有相同的主键外键关系；两子表同一列不能存在不同的默认约束关系；两子表不能同时存在具有自增约束的列；两子表存在的唯一约束将丢失。"
	},
	"RedundantColumnAdd": {
	"操作含义": "在目标表中冗余复制源表某列",
	"接口": "RedundantColumnAdd(SourceTable.Column, TargetTable)",
	"举例": "RedundantColumnAdd(customers.name, orders.customer_name)",
	"约束条件": "两表需包含外键关系"
	},
	"RedundantColumnDrop": {
	"操作含义": "删除表中的冗余列",
	"接口": "RedundantColumnDrop(Table.Column)",
	"举例": "RedundantColumnDrop(orders.customer_name)",
	"约束条件": "需要确保删除列后不丢失数据"
	}

}

## 经验

以下是一些进行Schema调整的成功经验

~~~
场景: 两个或多个表之间频繁进行等值连接，且连接条件中涉及的列选择性高，查询需要匹配的行是唯一的（一对一或一对多）。

操作: TableJoin(t1, t2, ..., join_key)

效果: 减少高频连接操作的执行开销，降低查询延迟。

场景: 一个非常宽的表，少数几列被高频查询，而另一些列或被低频访问。

操作: VerticalSplit(SourceTable, is_retained): table1(主键+高频列), table2(主键+低频/大字段列)

效果: 将高频查询所需的列集中到更紧凑的子表中，可能会降低查询延迟。

场景: 数据具有强烈的自然分区属性（如按年份、月份、租户ID），且绝大多数查询都附带针对该分区键的等值或范围过滤条件（如 WHERE year = 2024）。

操作: HorizontalSplit(SourceTable): Table1(分区依据1), Table2(分区依据2), ...

效果: 查询只需扫描特定分区，而非全表，减少数据扫描范围，提升了查询性能。

场景: 需要将多个按时间或业务分区的同构分表进行合并，以执行跨时间范围的查询。

操作: HorizontalMerge([分表1, 分表2, ...], is_retained): 新表

效果: 将多个分表逻辑或物理合并为一张表，使得分析查询无需跨多表UNION，简化了查询逻辑。

场景: 两个表因外键关系频繁连接，连接的目的仅是为了获取主表（如客户表）中的个别非关键属性（如客户姓名）到从表（如订单表）的查询结果中。

操作: RedundantColumnAdd(SourceTable.Column, TargetTable)

效果: 在从表中冗余存储所需属性，消除高频连接。


~~~

## 要求

现在，请给出你认为有助于在当前场景下缩短历史负载查询执行时间的Schema调整动作序列，要求：

~~~
1.按照支持的操作接口，给出操作序列，短横线分隔，无需回答其他内容
2.可参考给出的经验进行schema变化操作
3.每一项操作前后可能有表被删除，请根据操作顺序，在后续操作中使用变化后的新表进行操作  
4.在给出一个操作时，需要确定当前被操作的表和列经过前序操作仍包含其中  
~~~
