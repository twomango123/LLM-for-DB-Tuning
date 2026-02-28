SELECT DISTINCT c.customer_name
FROM customers AS c
WHERE NOT EXISTS (
  SELECT 1
  FROM customer_full_addresses AS ca
  WHERE ca.customer_id = c.customer_id
    AND ca.state_province_county = 'California'
);
