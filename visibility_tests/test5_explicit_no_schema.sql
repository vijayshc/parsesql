-- Test 5: Explicit columns in subquery - should work without schema
select a, b from (
    select x as a, y as b from unknown_table
) sub;