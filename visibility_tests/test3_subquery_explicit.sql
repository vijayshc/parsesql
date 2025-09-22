-- Test 3: Explicit columns in subquery should be visible to outer level
select first_name1, last_name2 from (
    select first_name1, last_name2 from customers1
) sub;