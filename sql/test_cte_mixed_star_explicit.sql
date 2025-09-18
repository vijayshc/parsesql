WITH base AS (
  SELECT customer_id, first_name, last_name FROM customers
), proj AS (
  SELECT customer_id, first_name FROM base
)
SELECT customer_id, *, first_name FROM proj;