SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM order_view WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? AND O_ID = (SELECT MAX(O_ID) FROM tpcch.orders WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ?);
