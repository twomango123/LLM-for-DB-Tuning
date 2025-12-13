select count(*) from tpcch.stock,(select distinct OL_I_ID from tpcch.orderline where OL_W_ID=? and OL_D_ID=? and OL_O_ID<? and OL_O_ID>=?) _ where S_I_ID=OL_I_ID and S_W_ID=? and S_QUANTITY<?;
