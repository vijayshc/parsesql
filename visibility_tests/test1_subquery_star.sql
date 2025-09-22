-- Test 1: * expansion in subquery should make all columns visible to outer level
select first_name1, last_name2 from (
    select * from customers1
) sub;