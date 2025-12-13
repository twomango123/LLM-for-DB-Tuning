-- TPC-H Query 11
-- ========================================
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
