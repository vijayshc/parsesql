WITH base AS (
  SELECT customer_id, first_name FROM customers
), wrapped AS (
  SELECT * FROM (
    SELECT customer_id, first_name FROM base WHERE customer_id > 100
  ) sq
), final AS (
  SELECT * FROM wrapped
)
SELECT * FROM final;