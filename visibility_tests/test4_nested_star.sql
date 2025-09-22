-- Test 4: Nested subqueries with * expansion
select first_name1, last_name2 from (
    select * from (
        select * from customers1
    ) inner_sub
) outer_sub;