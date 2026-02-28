UPDATE products_summary
SET product_price = ROUND(product_price * 0.95, 2)
ORDER BY product_price ASC, product_id ASC
LIMIT 1;

UPDATE products_descr
SET product_description = CONCAT(product_description, ' (promo)')
WHERE product_id = (
  SELECT t.product_id
  FROM (
    SELECT product_id
    FROM products_descr
    ORDER BY product_id DESC
    LIMIT 1
  ) AS t
);
