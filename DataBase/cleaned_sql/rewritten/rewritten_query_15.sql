-- TPC-H Query 15
-- ========================================
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
