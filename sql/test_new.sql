WITH customers1 AS (
	SELECT * FROM customers
), customers2 AS (
	SELECT *
	FROM customers1 a
	INNER JOIN orders b
		ON a.customer_id = b.customer_id
), customre3 AS (
	SELECT * FROM customers2
)
SELECT * FROM customre3;