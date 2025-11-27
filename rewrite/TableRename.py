from base import SMO
import re


# 表重命名
class TableRename(SMO):
    def __init__(self, old, new):
        self.old = old
        self.new = new

    def apply_to_schema(self, schema):
        pass

    def apply_to_sql(self, sql):
        pattern = rf"\b{re.escape(self.old)}\b"
        new_sql = re.sub(pattern, self.new, sql, flags=re.IGNORECASE)
        return new_sql

    def apply_to_data(self, row):
        return row  # 不改变 tuple
sql = """select
	c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount)
from
	tpcch.customer, tpcch.order, tpcch.orderline
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
"""
smo = TableRename("customer", "cc")
print(smo.apply_to_sql(sql))