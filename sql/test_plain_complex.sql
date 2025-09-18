SELECT
  c.customer_id,
  UPPER(c.first_name) AS first_name_upper,
  o.order_id,
  o.total_amount * 1.1 AS gross_amount,
  CASE WHEN o.total_amount > 100 THEN 'LARGE' ELSE 'SMALL' END AS size_flag,
  t.rank_in_group
FROM (
  SELECT customer_id, first_name FROM customers WHERE status = 'active'
) c
JOIN (
  SELECT order_id, customer_id, total_amount FROM orders WHERE order_date >= '2023-01-01'
) o ON c.customer_id = o.customer_id
LEFT JOIN (
  SELECT customer_id, order_id,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_amount DESC) AS rank_in_group
  FROM orders
) t ON t.customer_id = c.customer_id AND t.order_id = o.order_id
WHERE o.total_amount IS NOT NULL
UNION ALL
SELECT
  c2.customer_id,
  c2.first_name AS first_name_upper,
  NULL AS order_id,
  0 AS gross_amount,
  'NONE' AS size_flag,
  NULL AS rank_in_group
FROM customers c2
WHERE c2.status = 'inactive';