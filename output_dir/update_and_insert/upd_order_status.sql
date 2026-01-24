-- Move the newest actual order from NEW to PACKED
UPDATE actual_orders
SET order_status_code = 'PACKED'
WHERE actual_order_id = (SELECT MAX(actual_order_id) FROM actual_orders)
  AND order_status_code = 'NEW';

