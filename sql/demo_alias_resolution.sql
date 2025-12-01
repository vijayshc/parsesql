-- Demo SQL showcasing alias resolution and target column tracking
-- Test 1: Simple JOIN with aliases
SELECT 
    c.customer_id,
    c.first_name,
    o.order_id,
    o.total_amount
FROM customers c
INNER JOIN orders o 
ON c.customer_id = o.customer_id
WHERE c.status = 'active' AND o.total_amount > 100;
