SELECT t2.state_province_county ,  count(*) FROM customer_addresses_extended AS t1 JOIN addresses AS t2 ON t1.address_id  =  t2.address_id GROUP BY t2.state_province_county;
