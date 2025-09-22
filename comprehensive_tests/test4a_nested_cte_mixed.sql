-- Test 4a: Nested CTEs with mixed schema/no-schema
with level1 as (
    select * from customers  -- has schema
),
level2 as (
    select customer_id, first_name from level1
),
level3 as (
    select customer_id from level2
)
select customer_id from level3;