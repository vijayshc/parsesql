-- Test 4c: CTE with subquery JOIN
with base_cte as (
    select explicit_col from unknown_table10
)
select base_cte.explicit_col, sub.inferred_col from base_cte
join (select * from unknown_table11) sub on base_cte.explicit_col = sub.link_col;