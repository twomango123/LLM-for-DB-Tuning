-- Update latest customer's contact info (demo)
UPDATE customers
SET customer_phone = '+1-555-0199',
    customer_email = 'support@acme.example'
WHERE customer_id = (SELECT MAX(customer_id) FROM customers);

