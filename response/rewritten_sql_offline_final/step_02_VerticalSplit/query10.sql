SELECT state_province_county FROM customer_addresses_extended WHERE address_id NOT IN (SELECT employee_address_id FROM employees);
