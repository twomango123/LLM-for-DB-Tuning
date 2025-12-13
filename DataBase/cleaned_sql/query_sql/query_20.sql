-- TPC-H Query 20
-- ========================================
select	 su_name, su_address
from	 tpcch.supplier, tpcch.nation
where	 su_suppkey in
		(select  mod(s_i_id * s_w_id, 10000)
		from     tpcch.stock, tpcch.orderline
		where    s_i_id in
				(select i_id
				 from tpcch.item
				 where i_data like 'co%')
			 and ol_i_id=s_i_id
			 and ol_delivery_d > '2010-05-23 12:00:00'
		group by s_i_id, s_w_id, s_quantity
		having   2*s_quantity > sum(ol_quantity))
	 and su_nationkey = n_nationkey
	 and n_name = 'GERMANY'
order by su_name;
