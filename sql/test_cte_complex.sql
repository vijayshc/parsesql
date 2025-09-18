WITH base AS (
  SELECT *  FROM customers
), lvl1 AS (
  SELECT * FROM base WHERE status = 'active'
), lvl2 AS (
  SELECT * FROM lvl1
), lvl3 AS (
  SELECT * FROM lvl2
), lvl4 AS (
  SELECT * FROM lvl3
), lvl5 AS (
  SELECT * FROM lvl4
)
SELECT * FROM lvl5;
