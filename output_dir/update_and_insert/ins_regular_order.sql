-- Create a new regular order for the latest customer (as distributor)
INSERT INTO regular_orders (
  distributer_id
) VALUES (
  (SELECT MAX(customer_id) FROM customers)
);

