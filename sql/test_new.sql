WITH customers1 AS (
	SELECT a.*,b.order_id FROM customers a
    inner join orders b on a.customer_id = b.customer_id
)
insert into orders
SELECT * FROM customers1;