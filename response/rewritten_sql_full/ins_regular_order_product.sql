INSERT INTO regular_order_products (
  regular_order_id,
  product_id
) VALUES (
  (SELECT MAX(regular_order_id) FROM regular_orders),
  (SELECT product_id FROM products_summary ORDER BY product_price ASC, product_id ASC LIMIT 1)
);
