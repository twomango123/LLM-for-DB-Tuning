-- Mark newest delivery as IN_TRANSIT
UPDATE order_deliveries
SET delivery_status_code = 'IN_TRANSIT',
    delivery_date = NOW()
WHERE (actual_order_id, delivery_date) IN (
  SELECT actual_order_id, MAX(delivery_date)
  FROM order_deliveries
  GROUP BY actual_order_id
)
AND delivery_status_code = 'SCHEDULED';

