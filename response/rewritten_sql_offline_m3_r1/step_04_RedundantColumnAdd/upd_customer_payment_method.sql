UPDATE customers
SET payment_method = 'CARD'
WHERE payment_method <> 'CARD'
ORDER BY customer_id DESC
LIMIT 1;
