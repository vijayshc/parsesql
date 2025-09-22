-- Test file to identify gaps in SQL construct support

-- 1. Window functions
CREATE TABLE window_test AS
SELECT 
    customer_id,
    first_name,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY customer_id) as rn,
    LEAD(customer_id) OVER (ORDER BY customer_id) as next_id,
    SUM(customer_id) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_sum
FROM customers;

-- 2. CASE statements
INSERT INTO target_table
SELECT 
    customer_id,
    CASE 
        WHEN status = 'active' THEN first_name
        WHEN status = 'inactive' THEN last_name
        ELSE CONCAT(first_name, last_name)
    END as processed_name
FROM customers;

-- 3. Lateral joins (if supported by engine)
SELECT c.customer_id, t.col_value
FROM customers c
LATERAL VIEW explode(array('a','b','c')) t as col_value;

-- 4. Recursive CTEs
WITH RECURSIVE hierarchy AS (
    SELECT customer_id, first_name, 0 as level
    FROM customers
    WHERE status = 'root'
    UNION ALL
    SELECT c.customer_id, c.first_name, h.level + 1
    FROM customers c
    JOIN hierarchy h ON c.status = CAST(h.customer_id AS STRING)
)
SELECT * FROM hierarchy;

-- 5. PIVOT operations
SELECT * FROM (
    SELECT customer_id, status, first_name
    FROM customers
) PIVOT (
    MAX(first_name) FOR status IN ('active' as active_name, 'inactive' as inactive_name)
);

-- 6. UNPIVOT operations  
SELECT customer_id, status_type, name_value
FROM (
    SELECT customer_id, first_name, last_name
    FROM customers
) UNPIVOT (
    name_value FOR status_type IN (first_name as 'first', last_name as 'last')
);

-- 7. Table functions
SELECT customer_id, pos, val
FROM customers
LATERAL VIEW posexplode(split(first_name, '')) t as pos, val;

-- 8. GROUPING SETS, CUBE, ROLLUP
SELECT status, COUNT(*) as cnt
FROM customers
GROUP BY GROUPING SETS ((status), ());

SELECT status, first_name, COUNT(*) as cnt
FROM customers  
GROUP BY CUBE (status, first_name);

SELECT status, first_name, COUNT(*) as cnt
FROM customers
GROUP BY ROLLUP (status, first_name);

-- 9. Multiple CTEs with dependencies
WITH base AS (
    SELECT customer_id, UPPER(first_name) as upper_name
    FROM customers
),
derived AS (
    SELECT customer_id, CONCAT(upper_name, '_processed') as final_name
    FROM base
)
INSERT INTO processed_customers
SELECT customer_id, final_name
FROM derived;

-- 10. Correlated subqueries
UPDATE customers 
SET status = 'updated'
WHERE customer_id IN (
    SELECT c2.customer_id 
    FROM customers c2 
    WHERE c2.first_name = customers.first_name
    AND c2.customer_id != customers.customer_id
);

-- 11. EXISTS/NOT EXISTS
SELECT c1.*
FROM customers c1
WHERE EXISTS (
    SELECT 1 
    FROM customers c2 
    WHERE c2.first_name = c1.first_name 
    AND c2.customer_id != c1.customer_id
);

-- 12. Set operations with multiple branches
SELECT customer_id, first_name FROM customers WHERE status = 'active'
UNION
SELECT customer_id, last_name FROM customers WHERE status = 'inactive'  
EXCEPT
SELECT customer_id, first_name FROM customers WHERE first_name LIKE 'A%'
INTERSECT
SELECT customer_id, first_name FROM customers WHERE LENGTH(first_name) > 3;