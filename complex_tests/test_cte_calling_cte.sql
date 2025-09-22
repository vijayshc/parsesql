-- Complex Test 5: CTE referencing another CTE that uses subqueries
with level1_cte as (
    select derived_id, derived_name from (
        select raw_id as derived_id, raw_name as derived_name from raw_data
    ) sub1
),
level2_cte as (
    select l1.derived_id, l1.derived_name, enriched_value
    from level1_cte l1
    join (select id, processed_value as enriched_value from processing_table) proc
    on l1.derived_id = proc.id
)
select final_id, final_name, final_value from (
    select derived_id as final_id, derived_name as final_name, enriched_value as final_value
    from level2_cte
) final_wrapper;