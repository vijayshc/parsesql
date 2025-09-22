-- Test 1: Simple SELECT with function
SELECT UPPER(first_name) FROM customers;

-- Test 2: SELECT with multiple functions
SELECT CONCAT(first_name, ' ', last_name), LENGTH(status) FROM customers;

-- Test 3: CTAS with CTE and complex expressions
CREATE TABLE derived_customers AS
WITH formatted AS (
    SELECT CONCAT(UPPER(first_name), ' ', LOWER(last_name)) AS full_name, 
           customer_id * 2 AS doubled_id
    FROM customers
)
SELECT * FROM formatted;

-- Test 4: INSERT with transformations
INSERT INTO orders 
SELECT customer_id, SUM(total_amount), COUNT(*), MAX(order_date) 
FROM orders 
GROUP BY customer_id;

-- Test 5: Complex CTE chain
CREATE TABLE complex_analysis AS
WITH base AS (
    SELECT customer_id, SUBSTR(first_name, 1, 3) AS short_name
    FROM customers
),
enhanced AS (
    SELECT customer_id, CONCAT(short_name, '_processed') AS final_name
    FROM base
),
final AS (
    SELECT customer_id, UPPER(final_name) AS display_name
    FROM enhanced
)
SELECT * FROM final;