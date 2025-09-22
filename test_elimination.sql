-- Test: Process of elimination logic
select target_col from unknown_table_a t1
join (select other_col1, other_col2 from unknown_table_b) t2 on t1.id = t2.id;