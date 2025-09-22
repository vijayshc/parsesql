-- Test 7: Nested explicit columns - should work without schema
select final_a, final_b from (
    select a as final_a, b as final_b from (
        select x as a, y as b from unknown_table
    ) inner_sub
) outer_sub;