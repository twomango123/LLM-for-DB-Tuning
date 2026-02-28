UPDATE actual_order_details
SET product_id = (
  SELECT rop.product_id
  FROM regular_order_products AS rop
  WHERE rop.regular_order_id = (SELECT MAX(regular_order_id) FROM regular_orders)
  ORDER BY rop.product_id ASC
  LIMIT 1
)
WHERE actual_order_id = (
  SELECT t.actual_order_id
  FROM (
    SELECT actual_order_id
    FROM actual_order_details
    ORDER BY actual_order_id DESC
    LIMIT 1
  ) AS t
)
AND product_id IS NULL;
