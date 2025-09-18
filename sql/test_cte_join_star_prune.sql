WITH src AS (
  SELECT * FROM customers
), j AS (
  SELECT *
  FROM src s
  JOIN orders o ON s.customer_id = o.customer_id
), projected AS (
  SELECT s_customer_id AS customer_id
  FROM (
    SELECT j.customer_id AS s_customer_id, j.order_id, j.total_amount FROM j
  ) q
), final AS (
  SELECT * FROM projected
)
SELECT * FROM final;