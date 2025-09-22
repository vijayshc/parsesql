-- Test 1a: Star expansion with schema present
select customer_id, first_name from (
    select * from customers
) sub;