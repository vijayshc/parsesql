-- Complex Test 4: Star expansion with some explicit column selections
with star_cte as (
    select * from base_table
),
explicit_cte as (
    select specific_col, another_col from other_table  
)
select base_col1, base_col2, specific_col, conflict_col
from star_cte s
join explicit_cte e on s.common_id = e.common_id
join (select conflict_col from conflict_table) c on s.common_id = c.common_id;