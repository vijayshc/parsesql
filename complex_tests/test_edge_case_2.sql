-- Edge Case 2: Multiple star expansions with conflicts
select common_col, unique_col1, unique_col2 from (
    select * from table_with_common_and_unique1
) s1
join (
    select * from table_with_common_and_unique2  
) s2 on s1.common_col = s2.common_col;