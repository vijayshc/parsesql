-- Test JOINs and WHERE clauses
SELECT 
    c.customer_id,
    c.first_name,
    o.order_id,
    o.total_amount
FROM customers c
INNER JOIN orders o ON c.customer_id = o.customer_id
WHERE c.status = 'active' AND o.total_amount > 100
