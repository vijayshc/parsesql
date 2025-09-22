-- Test 2: * expansion in CTE should make all columns visible to outer level
with cte as (
    select * from customers1
)
select first_name1, last_name2 from cte;