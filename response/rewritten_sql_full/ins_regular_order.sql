INSERT INTO regular_orders (
  distributer_id,
  customer_name
)
SELECT
  c.customer_id,
  c.customer_name
FROM customers AS c
ORDER BY c.customer_id DESC
LIMIT 1;
