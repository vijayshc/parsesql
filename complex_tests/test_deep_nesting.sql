-- Complex Test 2: Deeply nested subqueries with column tracing
select final_id, final_name from (
    select level3_id as final_id, level3_name as final_name from (
        select level2_id as level3_id, level2_name as level3_name from (
            select level1_id as level2_id, level1_name as level2_name from (
                select original_id as level1_id, original_name as level1_name 
                from source_table
            ) level1
        ) level2  
    ) level3
) final_result;