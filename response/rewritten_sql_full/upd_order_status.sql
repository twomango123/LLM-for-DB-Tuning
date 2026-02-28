UPDATE actual_order_details
SET order_status_code = 'PACKED'
WHERE actual_order_id = (
  SELECT t.actual_order_id
  FROM (
    SELECT actual_order_id
    FROM actual_order_details
    WHERE order_status_code = 'NEW'
    ORDER BY actual_order_id DESC
    LIMIT 1
  ) AS t
);
