-- Complex Test 1: Nested CTEs calling other CTEs
with base_data as (
    select id, name, value from raw_table
),
filtered_data as (
    select id, name from base_data where value > 100
),
enriched_data as (
    select f.id, f.name, d.extra_info
    from filtered_data f
    join detail_table d on f.id = d.id
)
select id, name, extra_info from enriched_data;