WITH base AS (
  SELECT customer_id, first_name, last_name, status FROM customers
), lvl1 AS (
  -- Drop last_name and status
  SELECT customer_id, first_name FROM base
), lvl2 AS (
  SELECT * FROM lvl1
)

SELECT * FROM lvl2;