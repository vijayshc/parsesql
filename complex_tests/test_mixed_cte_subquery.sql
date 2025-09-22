-- Complex Test 3: Mixed CTE and subquery with explicit column selection
with main_cte as (
    select user_id, user_name from users_table
)
select u.user_id, u.user_name, p.profile_data, s.special_field
from main_cte u
join profile_table p on u.user_id = p.user_id
join (
    select user_id, important_data as special_field from special_table
) s on u.user_id = s.user_id;