-- Apply a small promo discount to a random low-priced product
UPDATE products
SET product_price = ROUND(product_price * 0.95, 2)
WHERE product_id = (
  SELECT product_id FROM products
  ORDER BY product_price ASC, product_id ASC
  LIMIT 1
);

