-- Test nested subqueries and complex expressions
SELECT c.customer_id,
       (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) as order_count,
       CASE 
           WHEN c.status = 'active' THEN UPPER(c.first_name) 
           ELSE LOWER(c.last_name) 
       END as conditional_name
FROM customers c;

-- Test star expansion with functions
CREATE TABLE summary AS
SELECT *, 
       CONCAT(first_name, last_name) as full_name,
       customer_id * 100 as scaled_id
FROM customers;

-- Test UNION with different expressions
SELECT UPPER(first_name) as name_field FROM customers WHERE status = 'active'
UNION ALL
SELECT LOWER(last_name) as name_field FROM customers WHERE status = 'inactive';