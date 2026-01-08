#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
import sys
import os
from typing import Optional

# Import PART1/2/3 from the same folder
_THIS_DIR = os.path.dirname(__file__)
if _THIS_DIR and _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from PART1 import build_part1
from PART2 import build_part2
from PART3 import build_part3


# 固定尾部：操作集合 / 经验 / 要求
TAIL_TEXT = """## 操作集合  
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
"""


def build_combined(schema_sql: str, csv_dir: str, sql_dir: str, latency_csv: Optional[str]) -> str:
    part1 = build_part1(schema_sql).rstrip()
    part2 = build_part2(csv_dir).rstrip()
    part3 = build_part3(sql_dir, latency_csv, unit="auto").rstrip()
    tail = TAIL_TEXT.rstrip()

    pieces = [part1, part2, part3, tail]
    return "\n\n".join(pieces) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser(
        description="COMBINATION: 拼接 PART1/2/3，并追加固定的“操作集合/经验/要求”尾部，写出完整提示词"
    )
    ap.add_argument("--schema-sql", required=True, help="schema.sql 路径（供 PART1 使用）")
    ap.add_argument("--csv-dir", required=True, help="包含 schema.sql 与 *.csv 的目录（供 PART2 使用）")
    ap.add_argument("--sql-dir", required=True, help="包含 queryN.sql 与延迟CSV的目录（供 PART3 使用）")
    ap.add_argument("--latency-csv", default=None, help="延迟CSV路径（可选；默认使用 sql_dir/latency_results.csv）")
    ap.add_argument("--out", required=True, help="输出完整提示词文件路径")
    args = ap.parse_args()

    content = build_combined(args.schema_sql, args.csv_dir, args.sql_dir, args.latency_csv)
    Path(args.out).write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
