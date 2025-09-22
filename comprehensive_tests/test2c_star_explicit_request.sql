-- Test 2c: Star expansion with explicit outer selection - inferable
select requested_col1, requested_col2 from (
    select * from unknown_table3
) sub;