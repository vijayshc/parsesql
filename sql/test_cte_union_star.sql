WITH c1 AS (
  SELECT customer_id, first_name FROM customers WHERE status = 'active'
), c2 AS (
  SELECT customer_id, first_name FROM customers WHERE status = 'inactive'
), unioned AS (
  SELECT * FROM c1
  UNION ALL
  SELECT * FROM c2
), final AS (
  SELECT * FROM unioned
)
SELECT * FROM final;