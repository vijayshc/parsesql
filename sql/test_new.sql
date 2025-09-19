WITH customers1 AS (
	SELECT a.*,b.* FROM customersx a
    inner join orders1 b on a.customer_id = b.customer_id
)
,customers2 AS (
  SELECT customer_name, customer_id,order_id FROM customers1 WHERE a.status = 'active'
)
insert into orders
SELECT * FROM customers2;