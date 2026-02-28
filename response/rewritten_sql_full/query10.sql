SELECT t.state_province_county
FROM (
  SELECT DISTINCT address_id, state_province_county
  FROM customer_full_addresses
) AS t
WHERE t.address_id NOT IN (SELECT employee_address_id FROM employees);
