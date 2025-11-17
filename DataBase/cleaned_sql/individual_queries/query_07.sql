-- TPC-H Query 7
-- ========================================
select
	su_nationkey as supp_nation,
	substr(c_state,1,1) as cust_nation,
	extract(year from o_entry_d) as l_year,
	sum(ol_amount) as revenue
from
	tpcch.supplier, tpcch.stock, tpcch.orderline, tpcch.order, tpcch.customer, tpcch.nation n1, tpcch.nation n2
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
