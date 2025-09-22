-- Test 5b: Mixed explicit and star in same query
select explicit_col, star_col from unknown_table13 t1
join (select explicit_col from unknown_table14) t2 on t1.key = t2.key
join (select * from unknown_table15) t3 on t1.key = t3.key;