WITH c1 AS (
  SELECT * FROM customers
), c2 AS (
  SELECT c1.customer_id, c1.first_name, c1.status FROM c1
), c3 AS (
  SELECT c2.customer_id, c2.first_name FROM c2  -- prune status
), c4 AS (
  SELECT c3.* FROM c3
), c5 AS (
  SELECT c4.customer_id FROM c4  -- prune first_name
)
SELECT * FROM c5;