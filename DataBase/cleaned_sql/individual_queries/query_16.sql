-- TPC-H Query 16
-- ========================================
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
