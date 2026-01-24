-- Schedule a delivery for the newest actual order to a known location, driver, and truck
INSERT INTO order_deliveries (
  location_code,
  actual_order_id,
  delivery_status_code,
  driver_employee_id,
  truck_id,
  delivery_date
) VALUES (
  (SELECT location_code FROM delivery_route_locations ORDER BY location_code ASC LIMIT 1),
  (SELECT MAX(actual_order_id) FROM actual_orders),
  'SCHEDULED',
  (SELECT MIN(employee_id) FROM employees),
  (SELECT MIN(truck_id) FROM trucks),
  NOW()
);

