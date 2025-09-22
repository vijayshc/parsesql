-- Test 3a: Star expansion in JOIN without schema - not inferable
select mystery_column from table_with_schema t1
join (select * from unknown_table4) t2 on t1.id = t2.id;