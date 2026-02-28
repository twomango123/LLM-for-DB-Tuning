SELECT payment_method FROM customers GROUP BY payment_method ORDER BY count(*) DESC LIMIT 1;
