-- Test 4c-modified: Unqualified reference to see if it behaves differently
with base_cte as (
    select explicit_col from unknown_table10
)
select explicit_col, inferred_col from base_cte
join (select * from unknown_table11) sub on base_cte.explicit_col = sub.link_col;