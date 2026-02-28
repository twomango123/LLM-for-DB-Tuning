背景：

你是一个数据库性能调优专家，需要进行数据库模式修改以提高系统的性能表现(降低查询延迟)。

信息：

数据库当前的模式为：

"products": {
	"product_id": {"INT", "len=4", "PRIMARY KEY"},
	"product_name": {"VARCHAR", "len=20"},
	"product_price": {"DECIMAL", "len=9"},
	"product_description": {"VARCHAR", "len=255"}
},
"addresses": {
	"address_id": {"INT", "len=4", "PRIMARY KEY"},
	"address_details": {"VARCHAR", "len=80"},
	"city": {"VARCHAR", "len=50"},
	"zip_postcode": {"VARCHAR", "len=20"},
	"state_province_county": {"VARCHAR", "len=50"},
	"country": {"VARCHAR", "len=50"}
},
"customers": {
	"customer_id": {"INT", "len=4", "PRIMARY KEY"},
	"payment_method": {"VARCHAR", "len=10"},
	"customer_name": {"VARCHAR", "len=80"},
	"customer_phone": {"VARCHAR", "len=80"},
	"customer_email": {"VARCHAR", "len=80"},
	"date_became_customer": {"DATETIME", "len=8"}
},
"regular_orders": {
	"regular_order_id": {"INT", "len=4", "PRIMARY KEY"},
	"distributer_id": {"INT", "len=4", "FOREIGN KEY REFERENCES customers(customer_id)"}
},
"regular_order_products": {
	"regular_order_id": {"INT", "len=4", "FOREIGN KEY REFERENCES regular_orders(regular_order_id)"},
	"product_id": {"INT", "len=4", "FOREIGN KEY REFERENCES products(product_id)"}
},
"actual_orders": {
	"actual_order_id": {"INT", "len=4", "PRIMARY KEY"},
	"order_status_code": {"VARCHAR", "len=10"},
	"regular_order_id": {"INT", "len=4", "FOREIGN KEY REFERENCES regular_orders(regular_order_id)"},
	"actual_order_date": {"DATETIME", "len=8"}
},
"actual_order_products": {
	"actual_order_id": {"INT", "len=4", "FOREIGN KEY REFERENCES actual_orders(actual_order_id)"},
	"product_id": {"INT", "len=4", "FOREIGN KEY REFERENCES products(product_id)"}
},
"customer_addresses": {
	"customer_id": {"INT", "len=4", "FOREIGN KEY REFERENCES customers(customer_id)"},
	"address_id": {"INT", "len=4", "FOREIGN KEY REFERENCES addresses(address_id)"},
	"date_from": {"DATETIME", "len=8"},
	"address_type": {"VARCHAR", "len=10"},
	"date_to": {"DATETIME", "len=8"}
},
"delivery_routes": {
	"route_id": {"INT", "len=4", "PRIMARY KEY"},
	"route_name": {"VARCHAR", "len=50"},
	"other_route_details": {"VARCHAR", "len=255"}
},
"delivery_route_locations": {
	"location_code": {"VARCHAR", "len=10", "PRIMARY KEY"},
	"route_id": {"INT", "len=4", "FOREIGN KEY REFERENCES delivery_routes(route_id)"},
	"location_address_id": {"INT", "len=4", "FOREIGN KEY REFERENCES addresses(address_id)"},
	"location_name": {"VARCHAR", "len=50"}
},
"trucks": {
	"truck_id": {"INT", "len=4", "PRIMARY KEY"},
	"truck_licence_number": {"VARCHAR", "len=20"},
	"truck_details": {"VARCHAR", "len=255"}
},
"employees": {
	"employee_id": {"INT", "len=4", "PRIMARY KEY"},
	"employee_address_id": {"INT", "len=4", "FOREIGN KEY REFERENCES addresses(address_id)"},
	"employee_name": {"VARCHAR", "len=80"},
	"employee_phone": {"VARCHAR", "len=80"}
},
"order_deliveries": {
	"location_code": {"VARCHAR", "len=10", "FOREIGN KEY REFERENCES delivery_route_locations(location_code)"},
	"actual_order_id": {"INT", "len=4", "FOREIGN KEY REFERENCES actual_orders(actual_order_id)"},
	"delivery_status_code": {"VARCHAR", "len=10"},
	"driver_employee_id": {"INT", "len=4", "FOREIGN KEY REFERENCES employees(employee_id)"},
	"truck_id": {"INT", "len=4", "FOREIGN KEY REFERENCES trucks(truck_id)"},
	"delivery_date": {"DATETIME", "len=8"}
}

表/列级操作与基数统计：

{
  "actual_orders": {
    "order_status_code": [
      {
        "operation": "scan",
        "rows": 98754.0,
        "avg_time": 61888.198757763974,
        "count": 644,
        "sum_time_ms": 39856000.0,
        "cost": 39856000.0
      },
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 0.0,
        "count": 484,
        "sum_time_ms": 0.0,
        "cost": 0.0
      }
    ],
    "insert": {
      "count": 484,
      "avg_time": 34.412145148962736,
      "cost": 16655.478252097964
    },
    "update": {
      "count": 484,
      "avg_time": 0.0,
      "cost": 0.0
    },
    "表行数": 987541
  },
  "products": {
    "product_id": [
      {
        "operation": "join(product_id)",
        "rows": 998447.0,
        "avg_time": 70499.96610006779,
        "count": 160,
        "sum_time_ms": 11279994.576010847,
        "cost": 11279994.576010847
      }
    ],
    "product_price": [
      {
        "operation": "order by",
        "rows": 924591.0,
        "avg_time": 60283.68794326241,
        "count": 1128,
        "sum_time_ms": 68000000.0,
        "cost": 68000000.0
      },
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 801.5596049372107,
        "count": 484,
        "sum_time_ms": 387954.84878960997,
        "cost": 387954.84878960997
      }
    ],
    "update": {
      "count": 484,
      "avg_time": 801.5596049372107,
      "cost": 387954.84878960997
    },
    "join": [
      {
        "table": "regular_order_products",
        "count": 160,
        "pairs": [
          [
            "product_id",
            "product_id"
          ]
        ]
      }
    ],
    "表行数": 924591
  },
  "regular_order_products": {
    "product_id": [
      {
        "operation": "join(product_id)",
        "rows": 998447.0,
        "avg_time": 70499.96609999999,
        "count": 160,
        "sum_time_ms": 11279994.576,
        "cost": 11279994.576
      }
    ],
    "insert": {
      "count": 484,
      "avg_time": 820.4697680193931,
      "cost": 397107.36772138625
    },
    "join": [
      {
        "table": "products",
        "count": 160,
        "pairs": [
          [
            "product_id",
            "product_id"
          ]
        ]
      }
    ],
    "表行数": 998447
  },
  "trucks": {
    "truck_licence_number": [
      {
        "operation": "order by",
        "rows": 998968.0,
        "avg_time": 174000.0,
        "count": 160,
        "sum_time_ms": 27840000.0,
        "cost": 27840000.0
      }
    ],
    "表行数": 998968
  },
  "customer_addresses": {
    "address_id": [
      {
        "operation": "join(address_id)",
        "rows": 994704.0,
        "avg_time": 349649.68295,
        "count": 480,
        "sum_time_ms": 167831847.81599998,
        "cost": 167831847.81599998
      }
    ],
    "customer_id": [
      {
        "operation": "join(customer_id)",
        "rows": 994704.0,
        "avg_time": 109349.90144999999,
        "count": 160,
        "sum_time_ms": 17495984.231999997,
        "cost": 17495984.231999997
      }
    ],
    "insert": {
      "count": 484,
      "avg_time": 17.818985041230917,
      "cost": 8624.388759955764
    },
    "join": [
      {
        "table": "addresses",
        "count": 480,
        "pairs": [
          [
            "address_id",
            "address_id"
          ]
        ]
      },
      {
        "table": "customers",
        "count": 160,
        "pairs": [
          [
            "customer_id",
            "customer_id"
          ]
        ]
      }
    ],
    "表行数": 994704
  },
  "addresses": {
    "address_id": [
      {
        "operation": "join(address_id)",
        "rows": 994704.0,
        "avg_time": 349649.6829503602,
        "count": 480,
        "sum_time_ms": 167831847.81617287,
        "cost": 167831847.81617287
      }
    ],
    "state_province_county": [
      {
        "operation": "scan",
        "rows": 95596.0,
        "avg_time": 4.170000000000001e-08,
        "count": 320,
        "sum_time_ms": 1.3344000000000003e-05,
        "cost": 1.3344000000000003e-05
      }
    ],
    "city": [
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 0.3459150902926922,
        "count": 484,
        "sum_time_ms": 167.42290370166302,
        "cost": 167.42290370166302
      }
    ],
    "zip_postcode": [
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 0.3459150902926922,
        "count": 484,
        "sum_time_ms": 167.42290370166302,
        "cost": 167.42290370166302
      }
    ],
    "insert": {
      "count": 484,
      "avg_time": 21.5022808406502,
      "cost": 10407.103926874697
    },
    "update": {
      "count": 484,
      "avg_time": 0.6918301805853844,
      "cost": 334.84580740332603
    },
    "join": [
      {
        "table": "customer_addresses",
        "count": 480,
        "pairs": [
          [
            "address_id",
            "address_id"
          ]
        ]
      }
    ],
    "表行数": 955963
  },
  "customers": {
    "payment_method": [
      {
        "operation": "group by",
        "rows": 998586.0,
        "avg_time": 260000.0,
        "count": 160,
        "sum_time_ms": 41600000.0,
        "cost": 41600000.0
      },
      {
        "operation": "scan",
        "rows": 99858.0,
        "avg_time": 47254.658385093164,
        "count": 644,
        "sum_time_ms": 30432000.0,
        "cost": 30432000.0
      },
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 0.0,
        "count": 484,
        "sum_time_ms": 0.0,
        "cost": 0.0
      }
    ],
    "customer_id": [
      {
        "operation": "join(customer_id)",
        "rows": 994704.0,
        "avg_time": 109349.90144999999,
        "count": 160,
        "sum_time_ms": 17495984.231999997,
        "cost": 17495984.231999997
      }
    ],
    "date_became_customer": [
      {
        "operation": "order by",
        "rows": 998586.0,
        "avg_time": 292000.0,
        "count": 320,
        "sum_time_ms": 93440000.0,
        "cost": 93440000.0
      }
    ],
    "customer_phone": [
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 5.265381536446512,
        "count": 484,
        "sum_time_ms": 2548.4446636401117,
        "cost": 2548.4446636401117
      }
    ],
    "customer_email": [
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 5.265381536446512,
        "count": 484,
        "sum_time_ms": 2548.4446636401117,
        "cost": 2548.4446636401117
      }
    ],
    "insert": {
      "count": 484,
      "avg_time": 11.569079011678696,
      "cost": 5599.434241652489
    },
    "update": {
      "count": 968,
      "avg_time": 5.265381536446512,
      "cost": 5096.889327280223
    },
    "join": [
      {
        "table": "customer_addresses",
        "count": 160,
        "pairs": [
          [
            "customer_id",
            "customer_id"
          ]
        ]
      }
    ],
    "表行数": 998586
  },
  "delivery_routes": {
    "route_name": [
      {
        "operation": "order by",
        "rows": 998968.0,
        "avg_time": 168000.0,
        "count": 160,
        "sum_time_ms": 26880000.0,
        "cost": 26880000.0
      }
    ],
    "route_id": [
      {
        "operation": "join(route_id)",
        "rows": 1.0,
        "avg_time": 11.370000000000003,
        "count": 160,
        "sum_time_ms": 1819.2000000000005,
        "cost": 1819.2000000000005
      }
    ],
    "join": [
      {
        "table": "delivery_route_locations",
        "count": 160,
        "pairs": [
          [
            "route_id",
            "route_id"
          ]
        ]
      }
    ],
    "表行数": 998968
  },
  "delivery_route_locations": {
    "route_id": [
      {
        "operation": "join(route_id)",
        "rows": 1.0,
        "avg_time": 0.690000000000003,
        "count": 160,
        "sum_time_ms": 110.4000000000005,
        "cost": 110.4000000000005
      }
    ],
    "join": [
      {
        "table": "delivery_routes",
        "count": 160,
        "pairs": [
          [
            "route_id",
            "route_id"
          ]
        ]
      }
    ],
    "表行数": 0
  },
  "order_deliveries": {
    "delivery_status_code": [
      {
        "operation": "scan",
        "rows": 99763.0,
        "avg_time": 0.0,
        "count": 968,
        "sum_time_ms": 0.0,
        "cost": 0.0
      },
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 0.0,
        "count": 968,
        "sum_time_ms": 0.0,
        "cost": 0.0
      }
    ],
    "delivery_date": [
      {
        "operation": "order by",
        "rows": 997633.0,
        "avg_time": 0.0,
        "count": 968,
        "sum_time_ms": 0.0,
        "cost": 0.0
      },
      {
        "operation": "update",
        "rows": 1,
        "avg_time": 0.0,
        "count": 968,
        "sum_time_ms": 0.0,
        "cost": 0.0
      }
    ],
    "insert": {
      "count": 484,
      "avg_time": 25.04113782197237,
      "cost": 12119.910705834627
    },
    "update": {
      "count": 968,
      "avg_time": 0.0,
      "cost": 0.0
    },
    "表行数": 997633
  },
  "regular_orders": {
    "insert": {
      "count": 484,
      "avg_time": 13.537860941141844,
      "cost": 6552.324695512652
    },
    "表行数": 998953
  },
  "actual_order_products": {
    "insert": {
      "count": 484,
      "avg_time": 29.24892702139914,
      "cost": 14156.480678357184
    },
    "表行数": 995531
  }
}

单查询中每个表中同时出现的列集合 频次统计：

actual_order_products:
<actual_order_id, product_id> count :1

actual_orders:
<actual_order_id, order_status_code> count :2
<actual_order_date, order_status_code, regular_order_id> count :1

addresses:
<address_id, state_province_county> count :4
<address_id, city, zip_postcode> count :1
<address_details, city, country, state_province_county, zip_postcode> count :1

customer_addresses:
<address_id> count :1
<address_id, customer_id> count :2
<address_id, address_type, customer_id, date_from, date_to> count :1

customers:
<payment_method> count :1
<customer_id, customer_name> count :1
<customer_id, payment_method> count :1
<customer_name, date_became_customer> count :1
<customer_email, customer_id, customer_phone> count :1
<customer_email, customer_name, payment_method> count :1
<customer_id, customer_name, customer_phone> count :1
<customer_email, customer_name, customer_phone, date_became_customer> count :1
<customer_email, customer_name, customer_phone, date_became_customer, payment_method> count :1

delivery_route_locations:
<location_code> count :1
<route_id> count :1

delivery_routes:
<route_name> count :1
<route_id, route_name> count :1

order_deliveries:
<actual_order_id, delivery_date, delivery_status_code> count :2
<actual_order_id, delivery_date, delivery_status_code, driver_employee_id, location_code, truck_id> count :1

products:
<product_id, product_price> count :2
<product_name, product_price> count :1
<product_id, product_name, product_price> count :1

regular_order_products:
<product_id> count :1
<product_id, regular_order_id> count :2

regular_orders:
<distributer_id> count :1

trucks:
<truck_details, truck_licence_number> count :1

## 操作集合  
{

	"ColumnRename": {
	"操作含义": "将一个属性列重命名",
	"接口": "ColumnRename(SourceTable.OldColumnName, NewColumnName)",
	"举例": "ColumnRename(users.email, user_email)"
	},
    
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
	"举例": "TableJoin(customer,customer_ext, c_id, ce_c_id, True):customer_all",
	},

	"HorizontalSplit": {
	"操作含义": "按谓词将表水平拆分成多个分表，可选保留或删除原表",
	"接口": "HorizontalSplit(SourceTable, is_retained):Table1(拆分依据),Table2(拆分依据),....",
	"举例": "HorizontalSplit(orders, False):orders_2023(year=2023), orders_2024(year=2024)",
	"约束条件": "当原表不保留，且表主键是其他表的外键时，允许操作，但操作会使其他表丢失外键约束。"
	},

	"HorizontalMerge": {
	"操作含义": "将同结构子表，水平合并为新表，可选保留或删除原表",
	"接口": "HorizontalMerge(Table1, Table2, is_retained):NewTable",
	"举例": "HorizontalMerge(orders_2023, orders_2024, False):orders_all",
	"约束条件": "两子表需具有相同的主键外键关系；两子表同一列不能存在不同的默认约束关系；两子表不能同时存在具有自增约束的列；两子表存在的唯一约束将丢失。"
	},
	"RedundantColumnAdd": {
	"操作含义": "在目标表中复制源表某个列作为一个新的冗余列，两表首先通过连接键连接",
	"接口": "RedundantColumnAdd(SourceTable.Column, TargetTable.RedundantColumnName, join_key)",
	"举例": "RedundantColumnAdd(customers.name, orders.customer_name, ['customers.customer_id=orders.customer_id'])",
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
1.按照支持的操作接口，每次回答都只给出完整操作序列，换行符分隔，无需回答其他内容
2.可参考给出的经验进行schema变化操作  
3.需要在历史负载查询执行时间更短时使用的存储空间尽可能小，请平衡两者代价  
4.需要注意读操作和写操作的频率，确保读写操作的总性能得到提升，请平衡两者代价
5.每一项操作前后可能有表被删除或增加，请根据操作顺序，在后续操作中使用变化后的新表进行操作  
6.在给出一个操作时，需要确定当前被操作的表和列经过前序操作仍包含其中  
~~~
