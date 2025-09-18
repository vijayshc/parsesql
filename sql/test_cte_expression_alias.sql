WITH base AS (
  SELECT customer_id, total_amount FROM orders
), metrics AS (
  SELECT customer_id, total_amount, total_amount * 0.1 AS tax, total_amount + 5 AS adjusted
  FROM base
), final AS (
  SELECT customer_id, tax, adjusted FROM metrics
)
SELECT * FROM final;