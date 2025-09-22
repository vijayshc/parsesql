-- Test 4b: Nested subqueries with star expansion
select final_col from (
    select inner_col as final_col from (
        select * from unknown_table9
    ) level2
) level1;