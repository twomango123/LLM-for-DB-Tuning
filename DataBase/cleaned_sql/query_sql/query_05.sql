-- TPC-H Query 5
-- ========================================
select
	n_name,
	sum(ol_amount) as revenue
from
	tpcch.customer, tpcch.order, tpcch.orderline, tpcch.stock, tpcch.supplier, tpcch.nation, tpcch.region
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
