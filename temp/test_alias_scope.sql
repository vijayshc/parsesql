-- Test case for alias scope issue
-- Inner subquery: c refers to customer
-- Outer query: c refers to account  
SELECT col1, col2
FROM (
    SELECT c.first_name as col1, c.last_name as col2
    FROM customer c
) c
JOIN account a ON c.col1 = a.customer_name;