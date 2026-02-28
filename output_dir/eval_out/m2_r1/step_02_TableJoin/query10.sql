SELECT state_province_county FROM addresses WHERE address_id NOT IN (SELECT employee_address_id FROM employees);
