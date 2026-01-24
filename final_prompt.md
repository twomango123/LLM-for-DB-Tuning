背景：

你是一个数据库性能调优专家，需要进行数据库模式修改以提高系统的性能表现(降低查询延迟)。

信息：

数据库当前的模式为：

"warehouse": {
	"w_id": "INTEGER",
	"w_name": "CHAR",
	"w_street_1": "CHAR",
	"w_street_2": "CHAR",
	"w_city": "CHAR",
	"w_state": "CHAR",
	"w_zip": "CHAR",
	"w_tax": "DECIMAL",
	"w_ytd": "DECIMAL"
},
"district": {
	"d_id": "TINYINT",
	"d_w_id": "INTEGER",
	"d_name": "CHAR",
	"d_street_1": "CHAR",
	"d_street_2": "CHAR",
	"d_city": "CHAR",
	"d_state": "CHAR",
	"d_zip": "CHAR",
	"d_tax": "DECIMAL",
	"d_ytd": "DECIMAL",
	"d_next_o_id": "INTEGER"
},
"customer": {
	"c_id": "SMALLINT",
	"c_d_id": "TINYINT",
	"c_w_id": "INTEGER",
	"c_first": "CHAR",
	"c_middle": "CHAR",
	"c_last": "CHAR",
	"c_street_1": "CHAR",
	"c_street_2": "CHAR",
	"c_city": "CHAR",
	"c_state": "CHAR",
	"c_zip": "CHAR",
	"c_phone": "CHAR",
	"c_since": "DATE",
	"c_credit": "CHAR",
	"c_credit_lim": "DECIMAL",
	"c_discount": "DECIMAL",
	"c_balance": "DECIMAL",
	"c_ytd_payment": "DECIMAL",
	"c_payment_cnt": "SMALLINT",
	"c_delivery_cnt": "SMALLINT",
	"c_data": "TEXT",
	"c_n_nationkey": "INTEGER"
},
"history": {
	"h_c_id": "SMALLINT",
	"h_c_d_id": "TINYINT",
	"h_c_w_id": "INTEGER",
	"h_d_id": "TINYINT",
	"h_w_id": "INTEGER",
	"h_date": "DATE",
	"h_amount": "DECIMAL",
	"h_data": "CHAR"
},
"neworder": {
	"no_o_id": "INTEGER",
	"no_d_id": "TINYINT",
	"no_w_id": "INTEGER"
},
"orders": {
	"o_id": "INTEGER",
	"o_d_id": "TINYINT",
	"o_w_id": "INTEGER",
	"o_c_id": "SMALLINT",
	"o_entry_d": "DATE",
	"o_carrier_id": "TINYINT",
	"o_ol_cnt": "TINYINT",
	"o_all_local": "TINYINT"
},
"orderline": {
	"ol_o_id": "INTEGER",
	"ol_d_id": "TINYINT",
	"ol_w_id": "INTEGER",
	"ol_number": "TINYINT",
	"ol_i_id": "INTEGER",
	"ol_supply_w_id": "INTEGER",
	"ol_delivery_d": "DATE",
	"ol_quantity": "SMALLINT",
	"ol_amount": "DECIMAL",
	"ol_dist_info": "CHAR"
},
"item": {
	"i_id": "INTEGER",
	"i_im_id": "SMALLINT",
	"i_name": "CHAR",
	"i_price": "DECIMAL",
	"i_data": "CHAR"
},
"stock": {
	"s_i_id": "INTEGER",
	"s_w_id": "INTEGER",
	"s_quantity": "INTEGER",
	"s_dist_01": "CHAR",
	"s_dist_02": "CHAR",
	"s_dist_03": "CHAR",
	"s_dist_04": "CHAR",
	"s_dist_05": "CHAR",
	"s_dist_06": "CHAR",
	"s_dist_07": "CHAR",
	"s_dist_08": "CHAR",
	"s_dist_09": "CHAR",
	"s_dist_10": "CHAR",
	"s_ytd": "INTEGER",
	"s_order_cnt": "INTEGER",
	"s_remote_cnt": "INTEGER",
	"s_data": "CHAR",
	"s_su_suppkey": "INTEGER"
},
"nation": {
	"n_nationkey": "TINYINT",
	"n_name": "CHAR",
	"n_regionkey": "TINYINT",
	"n_comment": "CHAR"
},
"supplier": {
	"su_suppkey": "SMALLINT",
	"su_name": "CHAR",
	"su_address": "CHAR",
	"su_nationkey": "TINYINT",
	"su_phone": "CHAR",
	"su_acctbal": "DECIMAL",
	"su_comment": "CHAR"
},
"region": {
	"r_regionkey": "TINYINT",
	"r_name": "CHAR",
	"r_comment": "CHAR"
}

列级操作与基数统计：

{
  "warehouse": {
    "w_id": [
      {
        "字段长度": 4
      }
    ],
    "w_name": [
      {
        "字段长度": 10
      }
    ],
    "w_street_1": [
      {
        "字段长度": 20
      }
    ],
    "w_street_2": [
      {
        "字段长度": 20
      }
    ],
    "w_city": [
      {
        "字段长度": 20
      }
    ],
    "w_state": [
      {
        "字段长度": 2
      }
    ],
    "w_zip": [
      {
        "字段长度": 9
      }
    ],
    "w_tax": [
      {
        "字段长度": 2
      }
    ],
    "w_ytd": [
      {
        "字段长度": 6
      }
    ]
  },
  "district": {
    "d_id": [
      {
        "字段长度": 1
      }
    ],
    "d_w_id": [
      {
        "字段长度": 4
      }
    ],
    "d_name": [
      {
        "字段长度": 10
      }
    ],
    "d_street_1": [
      {
        "字段长度": 20
      }
    ],
    "d_street_2": [
      {
        "字段长度": 20
      }
    ],
    "d_city": [
      {
        "字段长度": 20
      }
    ],
    "d_state": [
      {
        "字段长度": 2
      }
    ],
    "d_zip": [
      {
        "字段长度": 9
      }
    ],
    "d_tax": [
      {
        "字段长度": 2
      }
    ],
    "d_ytd": [
      {
        "字段长度": 6
      }
    ],
    "d_next_o_id": [
      {
        "字段长度": 4
      }
    ]
  },
  "customer": {
    "c_id": [
      {
        "字段长度": 2
      },
      {
        "operation": "filter(c_id = o_c_id)",
        "rows": 1,
        "avg_time": 40348.627174428155,
        "count": 18
      },
      {
        "operation": "group by",
        "rows": 288544.0,
        "filtered": 100.0,
        "avg_time": 1602695.238095238,
        "count": 9
      }
    ],
    "c_d_id": [
      {
        "字段长度": 1
      },
      {
        "operation": "filter(c_d_id = o_d_id)",
        "rows": 1,
        "avg_time": 40348.627174428155,
        "count": 18
      }
    ],
    "c_w_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(c_w_id = o_w_id)",
        "rows": 1,
        "avg_time": 40348.627174428155,
        "count": 18
      }
    ],
    "c_first": [
      {
        "字段长度": 16
      }
    ],
    "c_middle": [
      {
        "字段长度": 2
      }
    ],
    "c_last": [
      {
        "字段长度": 16
      },
      {
        "operation": "group by",
        "rows": 288544.0,
        "filtered": 100.0,
        "avg_time": 2357042.857142857,
        "count": 6
      }
    ],
    "c_street_1": [
      {
        "字段长度": 20
      }
    ],
    "c_street_2": [
      {
        "字段长度": 20
      }
    ],
    "c_city": [
      {
        "字段长度": 20
      },
      {
        "operation": "group by",
        "rows": 288544.0,
        "filtered": 100.0,
        "avg_time": 2685800.0,
        "count": 3
      }
    ],
    "c_state": [
      {
        "字段长度": 2
      },
      {
        "operation": "filter(c_state LIKE 'A%')",
        "rows": 32057.0,
        "filtered": 11.11,
        "avg_time": 72750.00001960987,
        "count": 3
      }
    ],
    "c_zip": [
      {
        "字段长度": 9
      }
    ],
    "c_phone": [
      {
        "字段长度": 16
      },
      {
        "operation": "group by",
        "rows": 288544.0,
        "filtered": 100.0,
        "avg_time": 2685800.0,
        "count": 3
      }
    ],
    "c_since": [
      {
        "字段长度": 3
      }
    ],
    "c_credit": [
      {
        "字段长度": 2
      }
    ],
    "c_credit_lim": [
      {
        "字段长度": 6
      }
    ],
    "c_discount": [
      {
        "字段长度": 2
      }
    ],
    "c_balance": [
      {
        "字段长度": 6
      },
      {
        "operation": "filter(c_balance > 0.00)",
        "rows": 96171.0,
        "filtered": 33.33,
        "avg_time": 192000.0,
        "count": 3
      }
    ],
    "c_ytd_payment": [
      {
        "字段长度": 6
      }
    ],
    "c_payment_cnt": [
      {
        "字段长度": 2
      }
    ],
    "c_delivery_cnt": [
      {
        "字段长度": 2
      }
    ],
    "c_data": [
      {
        "字段长度": 400
      }
    ],
    "c_n_nationkey": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(c_n_nationkey = su_nationkey)",
        "rows": 1,
        "avg_time": 4.700009782347852,
        "count": 3
      }
    ]
  },
  "history": {
    "h_c_id": [
      {
        "字段长度": 2
      }
    ],
    "h_c_d_id": [
      {
        "字段长度": 1
      }
    ],
    "h_c_w_id": [
      {
        "字段长度": 4
      }
    ],
    "h_d_id": [
      {
        "字段长度": 1
      }
    ],
    "h_w_id": [
      {
        "字段长度": 4
      }
    ],
    "h_date": [
      {
        "字段长度": 3
      }
    ],
    "h_amount": [
      {
        "字段长度": 3
      }
    ],
    "h_data": [
      {
        "字段长度": 24
      }
    ]
  },
  "neworder": {
    "no_o_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(no_o_id = o_id)",
        "rows": 1,
        "avg_time": 19000.000019609866,
        "count": 3
      }
    ],
    "no_d_id": [
      {
        "字段长度": 1
      },
      {
        "operation": "filter(no_d_id = o_d_id)",
        "rows": 1,
        "avg_time": 19000.000019609866,
        "count": 3
      }
    ],
    "no_w_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(no_w_id = o_w_id)",
        "rows": 1,
        "avg_time": 19000.000019609866,
        "count": 3
      }
    ]
  },
  "orders": {
    "o_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(o_id = ol_o_id)",
        "rows": 1,
        "avg_time": 9842.857142857143,
        "count": 3
      },
      {
        "operation": "group by",
        "rows": 297603.0,
        "filtered": 100.0,
        "avg_time": 1961285.7142857146,
        "count": 3
      }
    ],
    "o_d_id": [
      {
        "字段长度": 1
      },
      {
        "operation": "filter(o_d_id = c_d_id)",
        "rows": 1,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "filter(o_d_id = ol_d_id)",
        "rows": 1,
        "avg_time": 9842.857142857143,
        "count": 3
      },
      {
        "operation": "group by",
        "rows": 297603.0,
        "filtered": 100.0,
        "avg_time": 1961285.7142857146,
        "count": 3
      }
    ],
    "o_w_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(o_w_id = c_w_id)",
        "rows": 1,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "filter(o_w_id = ol_w_id)",
        "rows": 1,
        "avg_time": 9842.857142857143,
        "count": 3
      },
      {
        "operation": "group by",
        "rows": 10.0,
        "filtered": 100.0,
        "avg_time": 1961285.7142857146,
        "count": 3
      }
    ],
    "o_c_id": [
      {
        "字段长度": 2
      },
      {
        "operation": "filter(o_c_id = c_id)",
        "rows": 1,
        "avg_time": 0.0,
        "count": 3
      }
    ],
    "o_entry_d": [
      {
        "字段长度": 3
      },
      {
        "operation": "filter(o_entry_d < '2012-01-02 00:00:00.000000')",
        "rows": 99191.0,
        "filtered": 33.33,
        "avg_time": 9842.857142857143,
        "count": 3
      },
      {
        "operation": "filter(o_entry_d <= ol_delivery_d)",
        "rows": 1,
        "avg_time": 39157.28166666666,
        "count": 6
      },
      {
        "operation": "filter(o_entry_d > '2007-01-02 00:00:00.000000')",
        "rows": 99191.0,
        "filtered": 33.33,
        "avg_time": 19000.000019609866,
        "count": 3
      },
      {
        "operation": "filter(o_entry_d >= '2007-01-02 00:00:00.000000')",
        "rows": 99191.0,
        "filtered": 33.33,
        "avg_time": 53465.852386775376,
        "count": 9
      },
      {
        "operation": "filter(o_entry_d BETWEEN '2007-01-02 00:00:00.000000')",
        "rows": 1,
        "avg_time": 4.340679999999999,
        "count": 3
      },
      {
        "operation": "group by",
        "rows": 297603.0,
        "filtered": 100.0,
        "avg_time": 984842.8571428572,
        "count": 6
      },
      {
        "operation": "order by",
        "rows": 297603.0,
        "filtered": 100.0,
        "avg_time": 1000.0,
        "count": 3
      }
    ],
    "o_carrier_id": [
      {
        "字段长度": 1
      }
    ],
    "o_ol_cnt": [
      {
        "字段长度": 1
      },
      {
        "operation": "group by",
        "rows": 297603.0,
        "filtered": 100.0,
        "avg_time": 667297.7115873016,
        "count": 9
      },
      {
        "operation": "order by",
        "rows": 297603.0,
        "filtered": 100.0,
        "avg_time": 20303.71023809524,
        "count": 6
      }
    ],
    "o_all_local": [
      {
        "字段长度": 1
      }
    ]
  },
  "orderline": {
    "ol_o_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(ol_o_id = o_id)",
        "rows": 1,
        "avg_time": 7734.271171446976,
        "count": 27
      },
      {
        "operation": "group by",
        "rows": 2714705.0,
        "filtered": 100.0,
        "avg_time": 4200.0,
        "count": 3
      }
    ],
    "ol_d_id": [
      {
        "字段长度": 1
      },
      {
        "operation": "filter(ol_d_id = o_d_id)",
        "rows": 1,
        "avg_time": 7734.271171446976,
        "count": 27
      },
      {
        "operation": "group by",
        "rows": 2714705.0,
        "filtered": 100.0,
        "avg_time": 4200.0,
        "count": 3
      }
    ],
    "ol_w_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(ol_w_id = o_w_id)",
        "rows": 1,
        "avg_time": 7733.748948137826,
        "count": 27
      },
      {
        "operation": "filter(ol_w_id = s_w_id)",
        "rows": 1,
        "avg_time": 4.700009782347853,
        "count": 6
      },
      {
        "operation": "filter(ol_w_id IN (1,2,3)",
        "rows": 1,
        "avg_time": 250000.0,
        "count": 3
      },
      {
        "operation": "group by",
        "rows": 10.0,
        "filtered": 100.0,
        "avg_time": 4200.0,
        "count": 3
      }
    ],
    "ol_number": [
      {
        "字段长度": 1
      },
      {
        "operation": "group by",
        "rows": 2714705.0,
        "filtered": 100.0,
        "avg_time": 1202333.3333333333,
        "count": 3
      },
      {
        "operation": "order by",
        "rows": 2714705.0,
        "filtered": 100.0,
        "avg_time": 288333.3333333333,
        "count": 3
      }
    ],
    "ol_i_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(ol_i_id < 1000)",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "filter(ol_i_id = i_id)",
        "rows": 1,
        "avg_time": 764888.8888889456,
        "count": 9
      },
      {
        "operation": "filter(ol_i_id = s_i_id)",
        "rows": 1,
        "avg_time": 203720.87505381287,
        "count": 21
      }
    ],
    "ol_supply_w_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(ol_supply_w_id = s_w_id)",
        "rows": 1,
        "avg_time": 237734.26846428015,
        "count": 12
      }
    ],
    "ol_delivery_d": [
      {
        "字段长度": 3
      },
      {
        "operation": "filter(ol_delivery_d < '2020-01-01 00:00:00.000000')",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 121500.0,
        "count": 6
      },
      {
        "operation": "filter(ol_delivery_d < '2020-01-02 00:00:00.000000';)",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 275666.6666666667,
        "count": 3
      },
      {
        "operation": "filter(ol_delivery_d > '2007-01-02 00:00:00.000000')",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 457333.3333333333,
        "count": 3
      },
      {
        "operation": "filter(ol_delivery_d > '2010-05-23 12:00:00')",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 440500.0,
        "count": 3
      },
      {
        "operation": "filter(ol_delivery_d >= '1999-01-01 00:00:00.000000')",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 243000.0,
        "count": 3
      },
      {
        "operation": "filter(ol_delivery_d >= '2007-01-02 00:00:00.000000')",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 588000.000007,
        "count": 6
      },
      {
        "operation": "filter(ol_delivery_d >= o_entry_d)",
        "rows": 1,
        "avg_time": 0.0,
        "count": 3
      }
    ],
    "ol_quantity": [
      {
        "字段长度": 2
      },
      {
        "operation": "filter(ol_quantity <= 10)",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 250000.0,
        "count": 3
      },
      {
        "operation": "filter(ol_quantity >= 1)",
        "rows": 904811.0,
        "filtered": 33.33,
        "avg_time": 250000.0,
        "count": 3
      },
      {
        "operation": "filter(ol_quantity BETWEEN 1)",
        "rows": 1,
        "avg_time": 243000.0,
        "count": 3
      }
    ],
    "ol_amount": [
      {
        "字段长度": 3
      }
    ],
    "ol_dist_info": [
      {
        "字段长度": 24
      }
    ]
  },
  "item": {
    "i_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(i_id = m_i_id)",
        "rows": 1,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "filter(i_id = ol_i_id)",
        "rows": 1,
        "avg_time": 8.681359999999996,
        "count": 3
      },
      {
        "operation": "filter(i_id = s_i_id)",
        "rows": 1,
        "avg_time": 6674.444444444444,
        "count": 9
      },
      {
        "operation": "group by",
        "rows": 99504.0,
        "filtered": 100.0,
        "avg_time": 77000.0,
        "count": 3
      },
      {
        "operation": "order by",
        "rows": 99504.0,
        "filtered": 100.0,
        "avg_time": 1666.6666666666667,
        "count": 3
      }
    ],
    "i_im_id": [
      {
        "字段长度": 2
      }
    ],
    "i_name": [
      {
        "字段长度": 24
      },
      {
        "operation": "group by",
        "rows": 99504.0,
        "filtered": 100.0,
        "avg_time": 521633.3333333333,
        "count": 3
      }
    ],
    "i_price": [
      {
        "字段长度": 3
      },
      {
        "operation": "filter(i_price BETWEEN 1)",
        "rows": 1,
        "avg_time": 0.0,
        "count": 3
      }
    ],
    "i_data": [
      {
        "字段长度": 50
      },
      {
        "operation": "filter(i_data LIKE '%BB')",
        "rows": 11054.0,
        "filtered": 11.11,
        "avg_time": 59400.01082978723,
        "count": 3
      },
      {
        "operation": "filter(i_data LIKE '%a')",
        "rows": 11054.0,
        "filtered": 11.11,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "filter(i_data LIKE '%b')",
        "rows": 11054.0,
        "filtered": 11.11,
        "avg_time": 105668.11356005666,
        "count": 9
      },
      {
        "operation": "filter(i_data LIKE 'co%')",
        "rows": 11054.0,
        "filtered": 11.11,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "filter(i_data NOT LIKE 'zz%')",
        "rows": 88449.0,
        "filtered": 88.89,
        "avg_time": 20023.333333333332,
        "count": 3
      }
    ]
  },
  "stock": {
    "s_i_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "group by",
        "rows": 99069.0,
        "filtered": 100.0,
        "avg_time": 249833.33333333334,
        "count": 9
      }
    ],
    "s_w_id": [
      {
        "字段长度": 4
      },
      {
        "operation": "group by",
        "rows": 10.0,
        "filtered": 100.0,
        "avg_time": 4000.0,
        "count": 3
      }
    ],
    "s_quantity": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(s_quantity = m_s_quantity)",
        "rows": 1,
        "avg_time": 142500.0,
        "count": 3
      },
      {
        "operation": "group by",
        "rows": 955634.0,
        "filtered": 100.0,
        "avg_time": 4000.0,
        "count": 3
      }
    ],
    "s_dist_01": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_02": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_03": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_04": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_05": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_06": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_07": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_08": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_09": [
      {
        "字段长度": 24
      }
    ],
    "s_dist_10": [
      {
        "字段长度": 24
      }
    ],
    "s_ytd": [
      {
        "字段长度": 4
      }
    ],
    "s_order_cnt": [
      {
        "字段长度": 4
      }
    ],
    "s_remote_cnt": [
      {
        "字段长度": 4
      }
    ],
    "s_data": [
      {
        "字段长度": 50
      }
    ],
    "s_su_suppkey": [
      {
        "字段长度": 4
      },
      {
        "operation": "filter(s_su_suppkey = su_suppkey)",
        "rows": 1,
        "avg_time": 183801.13866172117,
        "count": 21
      },
      {
        "operation": "group by",
        "rows": 955634.0,
        "filtered": 100.0,
        "avg_time": 2554000.0,
        "count": 3
      }
    ]
  },
  "nation": {
    "n_nationkey": [
      {
        "字段长度": 1
      },
      {
        "operation": "filter(n_nationkey = c_n_nationkey)",
        "rows": 1,
        "avg_time": 3.843333333333334e-06,
        "count": 3
      }
    ],
    "n_name": [
      {
        "字段长度": 25
      },
      {
        "operation": "filter(n_name = 'GERMANY')",
        "rows": 6.0,
        "filtered": 10.0,
        "avg_time": 768022.4000002147,
        "count": 9
      },
      {
        "operation": "group by",
        "rows": 62.0,
        "filtered": 100.0,
        "avg_time": 901266.6666666667,
        "count": 9
      },
      {
        "operation": "order by",
        "rows": 62.0,
        "filtered": 100.0,
        "avg_time": 1333.3333333333333,
        "count": 6
      }
    ],
    "n_regionkey": [
      {
        "字段长度": 1
      },
      {
        "operation": "filter(n_regionkey = r_regionkey)",
        "rows": 1,
        "avg_time": 26502.11506080845,
        "count": 6
      }
    ],
    "n_comment": [
      {
        "字段长度": 152
      }
    ]
  },
  "supplier": {
    "su_suppkey": [
      {
        "字段长度": 2
      },
      {
        "operation": "filter(su_suppkey = supplier_no)",
        "rows": 1,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "order by",
        "rows": 9940.0,
        "filtered": 100.0,
        "avg_time": 7000.0,
        "count": 3
      }
    ],
    "su_name": [
      {
        "字段长度": 25
      },
      {
        "operation": "group by",
        "rows": 9940.0,
        "filtered": 100.0,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "order by",
        "rows": 9940.0,
        "filtered": 100.0,
        "avg_time": 555.5555555555555,
        "count": 9
      }
    ],
    "su_address": [
      {
        "字段长度": 40
      }
    ],
    "su_nationkey": [
      {
        "字段长度": 1
      },
      {
        "operation": "filter(su_nationkey = n_nationkey)",
        "rows": 1,
        "avg_time": 32934.04015867468,
        "count": 18
      },
      {
        "operation": "group by",
        "rows": 9940.0,
        "filtered": 100.0,
        "avg_time": 0.0,
        "count": 3
      },
      {
        "operation": "order by",
        "rows": 9940.0,
        "filtered": 100.0,
        "avg_time": 0.0,
        "count": 3
      }
    ],
    "su_phone": [
      {
        "字段长度": 15
      }
    ],
    "su_acctbal": [
      {
        "字段长度": 6
      }
    ],
    "su_comment": [
      {
        "字段长度": 101
      },
      {
        "operation": "filter(su_comment LIKE '%bad%')",
        "rows": 1104.0,
        "filtered": 11.11,
        "avg_time": 2080.0,
        "count": 3
      }
    ]
  },
  "region": {
    "r_regionkey": [
      {
        "字段长度": 1
      }
    ],
    "r_name": [
      {
        "字段长度": 55
      },
      {
        "operation": "filter(r_name = 'EUROPE')",
        "rows": 1.0,
        "filtered": 20.0,
        "avg_time": 75.32034489117393,
        "count": 6
      },
      {
        "operation": "filter(r_name LIKE 'EUROP%')",
        "rows": 1.0,
        "filtered": 20.0,
        "avg_time": 53099.800111834564,
        "count": 3
      }
    ],
    "r_comment": [
      {
        "字段长度": 152
      }
    ]
  }
}



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
	"举例": "TableJoin(customer,customer_ext, c_id, ce_c_id, True):customer_all",
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
