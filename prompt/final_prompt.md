背景：

你是一个数据库性能调优专家，需要进行数据库模式修改以提高系统的性能表现(降低查询延迟)。

信息：

数据库当前的模式为：

"Campuses": {
	"Id": "INT",
	"Campus": "TEXT",
	"Location": "TEXT",
	"County": "TEXT",
	"Year": "INT",
	"INSERT": "INTO"
},
"csu_fees": {
	"Campus": "INT",
	"Year": "INT",
	"CampusFee": "INT",
	"INSERT": "INTO"
},
"degrees": {
	"Year": "INT",
	"Campus": "INT",
	"Degrees": "INT",
	"INSERT": "INTO"
},
"discipline_enrollments": {
	"Campus": "INT",
	"Discipline": "INT",
	"Year": "INT",
	"Undergraduate": "INT",
	"Graduate": "INT",
	"INSERT": "INTO"
},
"enrollments": {
	"Campus": "INT",
	"Year": "INT",
	"TotalEnrollment_AY": "INT",
	"FTE_AY": "INT",
	"INSERT": "INTO"
},
"faculty": {
	"Campus": "INT",
	"Year": "INT",
	"Faculty": "DOUBLE",
	"INSERT": "INTO"
}

其中各表的行数从多到少分别为：

"Campuses": 20000 rows;
"csu_fees": 10000 rows;
"degrees": 10000 rows;
"discipline_enrollments": 10000 rows;
"enrollments": 10000 rows;
"faculty": 10000 rows;

历史负载及其在当前模式下的执行时间为：

~~~sql
-- SQL1 : N/A ms --
select
	ol_number,
	sum(ol_quantity) as sum_qty,
	sum(ol_amount) as sum_amount,
	avg(ol_quantity) as avg_qty, 
	avg(ol_amount) as avg_amount,
	count(*) as count_order
from
	tpcch.orderline
where
	ol_delivery_d > '2007-01-02 00:00:00.000000'
group by
	ol_number
order by
	ol_number;


-- SQL2 : N/A ms --
select
	su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
from
	tpcch.item, tpcch.supplier, tpcch.stock, tpcch.nation, tpcch.region,
	(	select
			s_i_id as m_i_id,
 			min(s_quantity) as m_s_quantity
		from
			tpcch.stock, tpcch.supplier, tpcch.nation, tpcch.region
		where
				s_su_suppkey = su_suppkey
			and su_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name like 'EUROP%'
		group by
			s_i_id
	) as son
where
		i_id = s_i_id
	and s_su_suppkey = su_suppkey
	and su_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and i_data like '%b'
	and r_name like 'EUROP%'
	and i_id = m_i_id
	and s_quantity = m_s_quantity
order by
	n_name, su_name, i_id;


-- SQL3 : N/A ms --
select
	ol_o_id, ol_w_id, ol_d_id,
	sum(ol_amount) as revenue, o_entry_d
from
	tpcch.customer, tpcch.neworder, tpcch.orders, tpcch.orderline
where
		c_state like 'A%'
	and c_id = o_c_id
	and c_w_id = o_w_id
	and c_d_id = o_d_id
	and no_w_id = o_w_id
	and no_d_id = o_d_id
	and no_o_id = o_id
	and ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_o_id = o_id
	and o_entry_d > '2007-01-02 00:00:00.000000'
group by
	ol_o_id, ol_w_id, ol_d_id, o_entry_d
order by
	revenue desc, o_entry_d;


-- SQL4 : N/A ms --
select
	o_ol_cnt, count(*) as order_count
from
	tpcch.orders
where
		o_entry_d >= '2007-01-02 00:00:00.000000'
	and o_entry_d < '2012-01-02 00:00:00.000000'
	and exists 
		(	select *
			from tpcch.orderline
			where 	o_id = ol_o_id
	    		and o_w_id = ol_w_id
	    		and o_d_id = ol_d_id
	    		and ol_delivery_d >= o_entry_d)
group by
	o_ol_cnt
order by
	o_ol_cnt;


-- SQL5 : N/A ms --
select
	n_name,
	sum(ol_amount) as revenue
from
	tpcch.customer, tpcch.orders, tpcch.orderline, tpcch.stock, tpcch.supplier, tpcch.nation, tpcch.region
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


-- SQL6 : N/A ms --
select
	sum(ol_amount) as revenue
from
	tpcch.orderline
where
		ol_delivery_d >= '1999-01-01 00:00:00.000000'
	and ol_delivery_d < '2020-01-01 00:00:00.000000'
	and ol_quantity between 1 and 100000;


-- SQL7 : N/A ms --
select
	su_nationkey as supp_nation,
	substr(c_state,1,1) as cust_nation,
	extract(year from o_entry_d) as l_year,
	sum(ol_amount) as revenue
from
	tpcch.supplier, tpcch.stock, tpcch.orderline, tpcch.orders, tpcch.customer, tpcch.nation n1, tpcch.nation n2
where
		ol_supply_w_id = s_w_id
	and ol_i_id = s_i_id
	and s_su_suppkey = su_suppkey
	and ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_o_id = o_id
	and c_id = o_c_id
	and c_w_id = o_w_id
	and c_d_id = o_d_id
	and su_nationkey = n1.n_nationkey
	and c_n_nationkey = n2.n_nationkey
	and (
		(n1.n_name = 'GERMANY' and n2.n_name = 'CAMBODIA')
		or
		(n1.n_name = 'CAMBODIA' and n2.n_name = 'GERMANY')
		)
	and ol_delivery_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'
group by
	su_nationkey, substr(c_state,1,1), extract(year from o_entry_d)
order by
	su_nationkey, cust_nation, l_year;


-- SQL8 : N/A ms --
select
	extract(year from o_entry_d) as l_year,
	sum(case when n2.n_name = 'GERMANY' then ol_amount else 0 end) / sum(ol_amount) as mkt_share
from
	tpcch.item, tpcch.supplier, tpcch.stock, tpcch.orderline, tpcch.orders, tpcch.customer, tpcch.nation n1, tpcch.nation n2, tpcch.region
where
		i_id = s_i_id
	and ol_i_id = s_i_id
	and ol_supply_w_id = s_w_id
	and s_su_suppkey = su_suppkey
	and ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_o_id = o_id
	and c_id = o_c_id
	and c_w_id = o_w_id
	and c_d_id = o_d_id
	and n1.n_nationkey = c_n_nationkey
	and n1.n_regionkey = r_regionkey
	and ol_i_id < 1000
	and r_name = 'EUROPE'
	and su_nationkey = n2.n_nationkey
	and o_entry_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'
	and i_data like '%b'
	and i_id = ol_i_id
group by
	extract(year from o_entry_d)
order by
	l_year;


-- SQL9 : N/A ms --
select
	n_name, extract(year from o_entry_d) as l_year, sum(ol_amount) as sum_profit
from
	tpcch.item, tpcch.stock, tpcch.supplier, tpcch.orderline, tpcch.orders, tpcch.nation
where
		ol_i_id = s_i_id
	and ol_supply_w_id = s_w_id
	and s_su_suppkey = su_suppkey
	and ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_o_id = o_id
	and ol_i_id = i_id
	and su_nationkey = n_nationkey
	and i_data like '%BB'
group by
	n_name, extract(year from o_entry_d)
order by
	n_name, l_year desc;


-- SQL10 : N/A ms --
select
	c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
from
	tpcch.customer, tpcch.orders, tpcch.orderline, tpcch.nation
where
		c_id = o_c_id
	and c_w_id = o_w_id
	and c_d_id = o_d_id
	and ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_o_id = o_id
	and o_entry_d >= '2007-01-02 00:00:00.000000'
	and o_entry_d <= ol_delivery_d
	and n_nationkey = c_n_nationkey
group by
	c_id, c_last, c_city, c_phone, n_name
order by
	revenue desc;


-- SQL11 : N/A ms --
select
	s_i_id, sum(s_order_cnt) as ordercount
from
	tpcch.stock, tpcch.supplier, tpcch.nation
where
		s_su_suppkey = su_suppkey
	and su_nationkey = n_nationkey
	and n_name = 'GERMANY'
group by
	s_i_id
having 
	sum(s_order_cnt) > (
		select
			sum(s_order_cnt) * .005
		from
			tpcch.stock, tpcch.supplier, tpcch.nation
		where
				s_su_suppkey = su_suppkey
			and su_nationkey = n_nationkey
			and n_name = 'GERMANY')
order by
	ordercount desc;


-- SQL12 : N/A ms --
select
	o_ol_cnt,
	sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
	sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
from
	tpcch.orders, tpcch.orderline
where
		ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_o_id = o_id
	and o_entry_d <= ol_delivery_d
	and ol_delivery_d < '2020-01-01 00:00:00.000000'
group by
	o_ol_cnt
order by
	o_ol_cnt;


-- SQL13 : N/A ms --
select
	c_count, count(*) as custdist
from
	(	select
			c_id, count(o_id) as c_count
		from
			tpcch.customer left outer join tpcch.orders on (
				c_w_id = o_w_id
			and c_d_id = o_d_id
			and c_id = o_c_id
			and o_carrier_id > 8)
	 	group by
	 		c_id
	 ) as c_orders
group by
	c_count
order by
	custdist desc, c_count desc;


-- SQL14 : N/A ms --
select
	100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue
from
	tpcch.orderline, tpcch.item
where
		ol_i_id = i_id
	and ol_delivery_d >= '2007-01-02 00:00:00.000000'
	and ol_delivery_d < '2020-01-02 00:00:00.000000';


-- SQL15 : N/A ms --
select
	su_suppkey, su_name, su_address, su_phone, total_revenue
from
	tpcch.supplier,
		(select
			s_su_suppkey as supplier_no,
			sum(ol_amount) as total_revenue
	 	from
	 		tpcch.orderline, tpcch.stock
		where
				ol_i_id = s_i_id
			and ol_supply_w_id = s_w_id
			and ol_delivery_d >= '2007-01-02 00:00:00.000000'
	 	group by
	 		s_su_suppkey
		) as revenue
where
		su_suppkey = supplier_no
	and total_revenue = (
		select max(total_revenue)
		from
			(select
				s_su_suppkey as supplier_no,
				sum(ol_amount) as total_revenue
	 		from
	 			tpcch.orderline, tpcch.stock
			where
					ol_i_id = s_i_id
				and ol_supply_w_id = s_w_id
				and ol_delivery_d >= '2007-01-02 00:00:00.000000'
	 		group by
	 			s_su_suppkey
		) as revenue
	) 
order by
	su_suppkey;


-- SQL16 : N/A ms --
select
	i_name,
	substr(i_data, 1, 3) as brand,
	i_price,
	count(distinct s_su_suppkey) as supplier_cnt
from
	tpcch.stock, tpcch.item
where
		i_id = s_i_id
	and i_data not like 'zz%'
	and (s_su_suppkey not in
		(	select
				su_suppkey
		 	from
		 		tpcch.supplier
		 	where
		 su_comment like '%bad%')
		)
group by
	i_name, substr(i_data, 1, 3), i_price
order by
	supplier_cnt desc;


-- SQL17 : N/A ms --
select
	sum(ol_amount) / 2.0 as avg_yearly
from
	tpcch.orderline,
	(	select
			i_id, avg(ol_quantity) as a
		from
			tpcch.item, tpcch.orderline
		    where
		    		i_data like '%b'
				and ol_i_id = i_id
		    group by
		    	i_id
	) t
where
		ol_i_id = t.i_id
	and ol_quantity < t.a;


-- SQL18 : N/A ms --
select
	c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount)
from
	tpcch.customer, tpcch.orders, tpcch.orderline
where
		c_id = o_c_id
	and c_w_id = o_w_id
	and c_d_id = o_d_id
	and ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_o_id = o_id
group by
	o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt
having
	sum(ol_amount) > 200
order by
	sum(ol_amount) desc, o_entry_d;


-- SQL19 : N/A ms --
select
	sum(ol_amount) as revenue
from
	tpcch.orderline, tpcch.item
where
	(
		ol_i_id = i_id
	and i_data like '%a'
	and ol_quantity >= 1
	and ol_quantity <= 10
	and i_price between 1 and 400000
	and ol_w_id in (1,2,3)
	) or (
		ol_i_id = i_id
	and i_data like '%b'
	and ol_quantity >= 1
	and ol_quantity <= 10
	and i_price between 1 and 400000
	and ol_w_id in (1,2,4)
	) or (
		ol_i_id = i_id
	and i_data like '%c'
	and ol_quantity >= 1
	and ol_quantity <= 10
	and i_price between 1 and 400000
	and ol_w_id in (1,5,3)
	);


-- SQL20 : N/A ms --
select	 su_name, su_address
from	 tpcch.supplier, tpcch.nation
where	 su_suppkey in
		(select  mod(s_i_id * s_w_id, 10000)
		from     tpcch.stock, tpcch.orderline
		where    s_i_id in
				(select i_id
				 from tpcch.item
				 where i_data like 'co%')
			 and ol_i_id=s_i_id
			 and ol_delivery_d > '2010-05-23 12:00:00'
		group by s_i_id, s_w_id, s_quantity
		having   2*s_quantity > sum(ol_quantity))
	 and su_nationkey = n_nationkey
	 and n_name = 'GERMANY'
order by su_name;


-- SQL21 : N/A ms --
select
	su_name, count(*) as numwait
from
	tpcch.supplier, tpcch.orderline l1, tpcch.orders, tpcch.stock, tpcch.nation
where
		ol_o_id = o_id
	and ol_w_id = o_w_id
	and ol_d_id = o_d_id
	and ol_w_id = s_w_id
	and ol_i_id = s_i_id
	and s_su_suppkey = su_suppkey
	and l1.ol_delivery_d > o_entry_d
	and not exists (
		select *
		from
			tpcch.orderline l2
		where
				l2.ol_o_id = l1.ol_o_id
			and l2.ol_w_id = l1.ol_w_id
			and l2.ol_d_id = l1.ol_d_id
			and l2.ol_delivery_d > l1.ol_delivery_d
		)
	and su_nationkey = n_nationkey
	and n_name = 'GERMANY'
group by
	su_name
order by
	numwait desc, su_name;


-- SQL22 : N/A ms --
select
	substr(c_state,1,1) as country,
	count(*) as numcust,
	sum(c_balance) as totacctbal
from
	tpcch.customer
where
		substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
	and c_balance > (
		select
			avg(c_BALANCE)
		from
			tpcch.customer
		where
				c_balance > 0.00
			and substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
	) 
	and not exists (
		select *
		from
			tpcch.orders
		where
				o_c_id = c_id
			and o_w_id = c_w_id
			and o_d_id = c_d_id
	) 
group by
	substr(c_state,1,1)
order by
	substr(c_state,1,1);
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
	"接口": "VerticalSplit(SourceTable, is_retained):table1(attribute1, ...),table2(attribute2, ...)",
	"举例": "VerticalSplit(CUSTOMER, True):C1(c_id,c_name,c_sex),C2(c_id,c_birthday,c_level)",
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
~~~
