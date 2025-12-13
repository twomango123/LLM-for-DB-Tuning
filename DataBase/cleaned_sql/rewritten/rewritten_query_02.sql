-- TPC-H Query 2
-- ========================================
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
