-- Test 2b: CTE with explicit columns - inferable
with data_cte as (
    select col1, col2 from unknown_table2
)
select col1, col2 from data_cte;