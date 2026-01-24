-- Update a customer's payment method (e.g., cash to card)
UPDATE customers
SET payment_method = 'CARD'
WHERE customer_id = (SELECT MAX(customer_id) FROM customers)
  AND payment_method <> 'CARD';

