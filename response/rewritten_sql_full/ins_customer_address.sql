UPDATE customer_full_addresses
SET customer_id = (SELECT MAX(customer_id) FROM customers),
    date_from = NOW(),
    address_type = 'BILLING',
    date_to = NULL
WHERE address_id = (
  SELECT t.address_id
  FROM (
    SELECT address_id
    FROM customer_full_addresses
    ORDER BY address_id DESC
    LIMIT 1
  ) AS t
);
