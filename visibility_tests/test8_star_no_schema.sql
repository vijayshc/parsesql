-- Test 8: * without schema - this is the challenging case
select col1, col2 from (
    select * from unknown_table
) sub;