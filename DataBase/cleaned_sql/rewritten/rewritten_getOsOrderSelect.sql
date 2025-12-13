select O_ID, O_ENTRY_D, O_CARRIER_ID from tpcch.order where O_W_ID=? and O_D_ID=? and O_C_ID=? and O_ID=(select max(O_ID) from tpcch.order where O_W_ID=? and O_D_ID=? and O_C_ID=?);
