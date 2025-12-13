select C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST from tpcch.customer where C_LAST=? and C_D_ID=? and C_W_ID=? order by C_FIRST asc;
