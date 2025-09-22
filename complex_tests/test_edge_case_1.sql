-- Edge Case 1: Recursive CTE with explicit override
with recursive_cte as (
    select base_id, computed_value from computation_table
),
override_cte as (
    select base_id from override_table  -- explicitly selects only base_id
)
select base_id, computed_value from recursive_cte r
join override_cte o on r.base_id = o.base_id;