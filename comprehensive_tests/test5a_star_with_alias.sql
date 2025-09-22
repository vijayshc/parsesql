-- Test 5a: Star expansion with column alias
select aliased_name from (
    select original_name as aliased_name from unknown_table12
) sub;