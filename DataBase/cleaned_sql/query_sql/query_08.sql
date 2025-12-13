-- TPC-H Query 8
-- ========================================
select
	extract(year from o_entry_d) as l_year,
	sum(case when n2.n_name = 'GERMANY' then ol_amount else 0 end) / sum(ol_amount) as mkt_share
from
	tpcch.item, tpcch.supplier, tpcch.stock, tpcch.orderline, tpcch.order, tpcch.customer, tpcch.nation n1, tpcch.nation n2, tpcch.region
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
