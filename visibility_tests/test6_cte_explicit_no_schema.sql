-- Test 6: CTE with explicit columns - should work without schema
with cte as (
    select col1, col2 from unknown_table
)
select col1, col2 from cte;