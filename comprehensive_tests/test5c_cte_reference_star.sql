-- Test 5c: CTE referencing another CTE with star
with cte1 as (
    select * from unknown_table16
),
cte2 as (
    select traced_col from cte1
)
select traced_col from cte2;