-- Test 1b: CTE with schema present
with customer_cte as (
    select * from customers
)
select customer_id, status from customer_cte;