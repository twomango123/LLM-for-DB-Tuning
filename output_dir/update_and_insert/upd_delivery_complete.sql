-- Mark newest delivery as DELIVERED
UPDATE order_deliveries
SET delivery_status_code = 'DELIVERED',
    delivery_date = NOW()
WHERE (actual_order_id, delivery_date) IN (
  SELECT actual_order_id, MAX(delivery_date)
  FROM order_deliveries
  GROUP BY actual_order_id
)
AND delivery_status_code = 'IN_TRANSIT';

