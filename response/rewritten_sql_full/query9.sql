SELECT t1.customer_name ,  t1.customer_phone FROM customers AS t1 JOIN customer_full_addresses AS t2 ON t1.customer_id  =  t2.customer_id WHERE t2.state_province_county  =  'California';
