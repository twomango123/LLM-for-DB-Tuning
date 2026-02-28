INSERT INTO actual_order_details (
  order_status_code,
  regular_order_id,
  actual_order_date,
  product_id
) VALUES (
  'NEW',
  (SELECT MAX(regular_order_id) FROM regular_orders),
  NOW(),
  NULL
);
