INSERT INTO actual_order_products (
  actual_order_id,
  product_id
)
SELECT 
  (SELECT MAX(actual_order_id) FROM actual_orders) AS actual_order_id,
  rop.product_id
FROM regular_order_products rop
WHERE rop.regular_order_id = (SELECT MAX(regular_order_id) FROM regular_orders)
ORDER BY rop.product_id ASC
LIMIT 1;

