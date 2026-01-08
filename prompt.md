# 生成Schema修改操作的提示词

## 背景

你是一个数据库性能调优专家，需要进行数据库模式修改以提高系统的性能表现(降低查询延迟)。

## 信息

数据库当前的模式为：

~~~sql
"Campuses": {
	"Id": "INT",
	"Campus": "TEXT",
	"Location": "TEXT",
	"County": "TEXT",
	"Year": "INT"
},
"csu_fees": {
	"Campus": "INT",
	"Year": "INT",
	"CampusFee": "INT"
},
"degrees": {
	"Year": "INT",
	"Campus": "INT",
	"Degrees": "INT"
},
"discipline_enrollments": {
	"Campus": "INT",
	"Discipline": "INT",
	"Year": "INT",
	"Undergraduate": "INT",
	"Graduate": "INT"
},
"enrollments": {
	"Campus": "INT",
	"Year": "INT",
	"TotalEnrollment_AY": "INT",
	"FTE_AY": "INT"
},
"faculty": {
	"Campus": "INT",
	"Year": "INT",
	"Faculty": "DOUBLE"
}
~~~
其中各表的行数从多到少分别为：

~~~python
"Campuses": 0 rows;
"csu_fees": 0 rows;
"degrees": 0 rows;
"discipline_enrollments": 0 rows;
"enrollments": 0 rows;
"faculty": 0 rows;
~~~
历史负载及其在当前模式下的执行时间为：

~~~sql
-- SQL1 : 7 seconds --
SELECT campus FROM campuses WHERE county  =  "Los Angeles";


-- SQL2 : 5 seconds --
SELECT campus FROM campuses WHERE county  =  "Los Angeles";


-- SQL3 : 5 seconds --
SELECT campus FROM campuses WHERE LOCATION  =  "Chico";


-- SQL4 : 5 seconds --
SELECT campus FROM campuses WHERE LOCATION  =  "Chico";


-- SQL5 : 5 seconds --
SELECT campus FROM campuses WHERE YEAR  =  1958;


-- SQL6 : 5 seconds --
SELECT campus FROM campuses WHERE YEAR  =  1958;


-- SQL7 : 5 seconds --
SELECT campus FROM campuses WHERE YEAR  <  1800;


-- SQL8 : 5 seconds --
SELECT campus FROM campuses WHERE YEAR  <  1800;


-- SQL9 : 5 seconds --
SELECT campus FROM campuses WHERE YEAR  >=  1935 AND YEAR  <=  1939;


-- SQL10 : 5 seconds --
SELECT campus FROM campuses WHERE YEAR  >=  1935 AND YEAR  <=  1939;


-- SQL11 : 60 seconds --
SELECT campus FROM campuses WHERE LOCATION  =  "Northridge" AND county  =  "Los Angeles" UNION SELECT campus FROM campuses WHERE LOCATION  =  "San Francisco" AND county  =  "San Francisco";


-- SQL12 : 11 seconds --
SELECT campus FROM campuses WHERE LOCATION  =  "Northridge" AND county  =  "Los Angeles" UNION SELECT campus FROM campuses WHERE LOCATION  =  "San Francisco" AND county  =  "San Francisco";


-- SQL13 : N/A seconds --
SELECT campusfee FROM campuses AS T1 JOIN csu_fees AS T2 ON T1.id  =  t2.campus WHERE t1.campus  =  "San Jose State University" AND T2.year  =  1996;


-- SQL14 : N/A seconds --
SELECT campusfee FROM campuses AS T1 JOIN csu_fees AS T2 ON T1.id  =  t2.campus WHERE t1.campus  =  "San Jose State University" AND T2.year  =  1996;


-- SQL15 : N/A seconds --
SELECT campusfee FROM campuses AS T1 JOIN csu_fees AS T2 ON T1.id  =  t2.campus WHERE t1.campus  =  "San Francisco State University" AND T2.year  =  1996;


-- SQL16 : N/A seconds --
SELECT campusfee FROM campuses AS T1 JOIN csu_fees AS T2 ON T1.id  =  t2.campus WHERE t1.campus  =  "San Francisco State University" AND T2.year  =  1996;


-- SQL17 : 4 seconds --
SELECT count(*) FROM csu_fees WHERE campusfee  >  (SELECT avg(campusfee) FROM csu_fees);


-- SQL18 : 4 seconds --
SELECT count(*) FROM csu_fees WHERE campusfee  >  (SELECT avg(campusfee) FROM csu_fees);


-- SQL19 : 4 seconds --
SELECT count(*) FROM csu_fees WHERE campusfee  >  (SELECT avg(campusfee) FROM csu_fees);


-- SQL20 : 4 seconds --
SELECT count(*) FROM csu_fees WHERE campusfee  >  (SELECT avg(campusfee) FROM csu_fees);


-- SQL21 : 6 seconds --
SELECT campus FROM campuses WHERE county  =  "Los Angeles" AND YEAR  >  1950;


-- SQL22 : 6 seconds --
SELECT campus FROM campuses WHERE county  =  "Los Angeles" AND YEAR  >  1950;


-- SQL23 : 3 seconds --
SELECT YEAR FROM degrees GROUP BY YEAR ORDER BY sum(degrees) DESC LIMIT 1;


-- SQL24 : 3 seconds --
SELECT YEAR FROM degrees GROUP BY YEAR ORDER BY sum(degrees) DESC LIMIT 1;


-- SQL25 : 14 seconds --
SELECT campus FROM degrees GROUP BY campus ORDER BY sum(degrees) DESC LIMIT 1;


-- SQL26 : 13 seconds --
SELECT campus FROM degrees GROUP BY campus ORDER BY sum(degrees) DESC LIMIT 1;


-- SQL27 : 6 seconds --
SELECT T1.campus FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2003 ORDER BY T2.faculty DESC LIMIT 1;


-- SQL28 : 5 seconds --
SELECT T1.campus FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2003 ORDER BY T2.faculty DESC LIMIT 1;


-- SQL29 : 2 seconds --
SELECT avg(campusfee) FROM csu_fees WHERE YEAR  =  1996;


-- SQL30 : 2 seconds --
SELECT avg(campusfee) FROM csu_fees WHERE YEAR  =  1996;


-- SQL31 : 2 seconds --
SELECT avg(campusfee) FROM csu_fees WHERE YEAR  =  2005;


-- SQL32 : 2 seconds --
SELECT avg(campusfee) FROM csu_fees WHERE YEAR  =  2005;


-- SQL33 : 8 seconds --
SELECT T1.campus ,  sum(T2.degrees) FROM campuses AS T1 JOIN degrees AS T2 ON T1.id  =  T2.campus WHERE T2.year  >=  1998 AND T2.year  <=  2002 GROUP BY T1.campus;


-- SQL34 : 5 seconds --
SELECT T1.campus ,  sum(T2.degrees) FROM campuses AS T1 JOIN degrees AS T2 ON T1.id  =  T2.campus WHERE T2.year  >=  1998 AND T2.year  <=  2002 GROUP BY T1.campus;


-- SQL35 : 6 seconds --
SELECT T1.campus ,  sum(T2.degrees) FROM campuses AS T1 JOIN degrees AS T2 ON T1.id  =  T2.campus WHERE T1.county  =  "Orange" AND T2.year  >=  2000 GROUP BY T1.campus;


-- SQL36 : 6 seconds --
SELECT T1.campus ,  sum(T2.degrees) FROM campuses AS T1 JOIN degrees AS T2 ON T1.id  =  T2.campus WHERE T1.county  =  "Orange" AND T2.year  >=  2000 GROUP BY T1.campus;


-- SQL37 : 10 seconds --
SELECT T1.campus FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2002 AND faculty  >  (SELECT max(faculty) FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2002 AND T1.county  =  "Orange");


-- SQL38 : 10 seconds --
SELECT T1.campus FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2002 AND faculty  >  (SELECT max(faculty) FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  T2.campus WHERE T2.year  =  2002 AND T1.county  =  "Orange");


-- SQL39 : N/A seconds --
SELECT T1.campus FROM campuses AS t1 JOIN enrollments AS t2 ON t1.id  =  t2.campus WHERE t2.year  =  1956 AND totalenrollment_ay  >  400 AND FTE_AY  >  200;


-- SQL40 : N/A seconds --
SELECT T1.campus FROM campuses AS t1 JOIN enrollments AS t2 ON t1.id  =  t2.campus WHERE t2.year  =  1956 AND totalenrollment_ay  >  400 AND FTE_AY  >  200;


-- SQL41 : 5 seconds --
SELECT count(*) FROM campuses WHERE county  =  "Los Angeles";


-- SQL42 : 5 seconds --
SELECT count(*) FROM campuses WHERE county  =  "Los Angeles";


-- SQL43 : 5 seconds --
SELECT campus FROM campuses WHERE county  =  "Los Angeles";


-- SQL44 : 5 seconds --
SELECT campus FROM campuses WHERE county  =  "Los Angeles";


-- SQL45 : N/A seconds --
SELECT degrees FROM campuses AS T1 JOIN degrees AS T2 ON t1.id  =  t2.campus WHERE t1.campus  =  "San Jose State University" AND t2.year  =  2000;


-- SQL46 : N/A seconds --
SELECT degrees FROM campuses AS T1 JOIN degrees AS T2 ON t1.id  =  t2.campus WHERE t1.campus  =  "San Jose State University" AND t2.year  =  2000;


-- SQL47 : N/A seconds --
SELECT degrees FROM campuses AS T1 JOIN degrees AS T2 ON t1.id  =  t2.campus WHERE t1.campus  =  "San Francisco State University" AND t2.year  =  2001;


-- SQL48 : N/A seconds --
SELECT degrees FROM campuses AS T1 JOIN degrees AS T2 ON t1.id  =  t2.campus WHERE t1.campus  =  "San Francisco State University" AND t2.year  =  2001;


-- SQL49 : 4 seconds --
SELECT sum(faculty) FROM faculty WHERE YEAR  =  2002;


-- SQL50 : 4 seconds --
SELECT sum(faculty) FROM faculty WHERE YEAR  =  2002;


-- SQL51 : 5 seconds --
SELECT faculty FROM faculty AS T1 JOIN campuses AS T2 ON T1.campus  =  T2.id WHERE T1.year  =  2002 AND T2.campus  =  "Long Beach State University";


-- SQL52 : 5 seconds --
SELECT faculty FROM faculty AS T1 JOIN campuses AS T2 ON T1.campus  =  T2.id WHERE T1.year  =  2002 AND T2.campus  =  "Long Beach State University";


-- SQL53 : 5 seconds --
SELECT faculty FROM faculty AS T1 JOIN campuses AS T2 ON T1.campus  =  T2.id WHERE T1.year  =  2004 AND T2.campus  =  "San Francisco State University";


-- SQL54 : 5 seconds --
SELECT faculty FROM faculty AS T1 JOIN campuses AS T2 ON T1.campus  =  T2.id WHERE T1.year  =  2004 AND T2.campus  =  "San Francisco State University";


-- SQL55 : N/A seconds --
SELECT T1.campus FROM campuses AS t1 JOIN faculty AS t2 ON t1.id  =  t2.campus WHERE t2.faculty  >=  600 AND t2.faculty  <=  1000 AND T1.year  =  2004;


-- SQL56 : N/A seconds --
SELECT T1.campus FROM campuses AS t1 JOIN faculty AS t2 ON t1.id  =  t2.campus WHERE t2.faculty  >=  600 AND t2.faculty  <=  1000 AND T1.year  =  2004;


-- SQL57 : N/A seconds --
SELECT T2.faculty FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  t2.campus JOIN degrees AS T3 ON T1.id  =  t3.campus AND t2.year  =  t3.year WHERE t2.year  =  2002 ORDER BY t3.degrees DESC LIMIT 1;


-- SQL58 : N/A seconds --
SELECT T2.faculty FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  t2.campus JOIN degrees AS T3 ON T1.id  =  t3.campus AND t2.year  =  t3.year WHERE t2.year  =  2002 ORDER BY t3.degrees DESC LIMIT 1;


-- SQL59 : N/A seconds --
SELECT T2.faculty FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  t2.campus JOIN degrees AS T3 ON T1.id  =  t3.campus AND t2.year  =  t3.year WHERE t2.year  =  2001 ORDER BY t3.degrees LIMIT 1;


-- SQL60 : N/A seconds --
SELECT T2.faculty FROM campuses AS T1 JOIN faculty AS T2 ON T1.id  =  t2.campus JOIN degrees AS T3 ON T1.id  =  t3.campus AND t2.year  =  t3.year WHERE t2.year  =  2001 ORDER BY t3.degrees LIMIT 1;


-- SQL61 : 6 seconds --
SELECT sum(t1.undergraduate) FROM discipline_enrollments AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t1.year  =  2004 AND t2.campus  =  "San Jose State University";


-- SQL62 : 5 seconds --
SELECT sum(t1.undergraduate) FROM discipline_enrollments AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t1.year  =  2004 AND t2.campus  =  "San Jose State University";


-- SQL63 : 5 seconds --
SELECT sum(t1.graduate) FROM discipline_enrollments AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t1.year  =  2004 AND t2.campus  =  "San Francisco State University";


-- SQL64 : 5 seconds --
SELECT sum(t1.graduate) FROM discipline_enrollments AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t1.year  =  2004 AND t2.campus  =  "San Francisco State University";


-- SQL65 : 5 seconds --
SELECT t1.campusfee FROM csu_fees AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t2.campus  =  "San Francisco State University" AND t1.year  =  2000;


-- SQL66 : 5 seconds --
SELECT t1.campusfee FROM csu_fees AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t2.campus  =  "San Francisco State University" AND t1.year  =  2000;


-- SQL67 : 5 seconds --
SELECT t1.campusfee FROM csu_fees AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t2.campus  =  "San Jose State University" AND t1.year  =  2000;


-- SQL68 : 5 seconds --
SELECT t1.campusfee FROM csu_fees AS t1 JOIN campuses AS t2 ON t1.campus  =  t2.id WHERE t2.campus  =  "San Jose State University" AND t1.year  =  2000;


-- SQL69 : 2 seconds --
SELECT count(*) FROM campuses;


-- SQL70 : 2 seconds --
SELECT count(*) FROM campuses;
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

---

