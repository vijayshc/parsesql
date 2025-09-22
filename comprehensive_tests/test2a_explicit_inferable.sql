-- Test 2a: Explicit column selection - inferable
select id, name from (
    select id, name from unknown_table1
) sub;