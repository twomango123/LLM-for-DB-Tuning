SELECT DISTINCT c.customer_name
FROM customers AS c
WHERE NOT EXISTS (
  SELECT 1
  FROM customer_addresses AS ca
  JOIN addresses AS a ON ca.address_id = a.address_id
  WHERE ca.customer_id = c.customer_id
    AND a.state_province_county = 'California'
);