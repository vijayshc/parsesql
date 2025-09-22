-- Test complex expression chains
CREATE TABLE complex_test AS
WITH base AS (
    SELECT CONCAT(first_name, ' ', last_name) AS full_name,
           UPPER(status) AS status_upper
    FROM customers
),
enhanced AS (
    SELECT CONCAT(full_name, ' - ', status_upper) AS display_name
    FROM base
)
SELECT * FROM enhanced;