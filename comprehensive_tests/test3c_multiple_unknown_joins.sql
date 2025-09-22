-- Test 3c: Multiple JOINs with unknown tables - not inferable
select ambiguous_col from unknown_table6 t1
join unknown_table7 t2 on t1.id = t2.id
join unknown_table8 t3 on t1.id = t3.id;