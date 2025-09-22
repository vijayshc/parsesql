-- Test case where CTEs are properly used
create table test_proper as
with base as (
    select c.first_name, c.last_name from customer c
),
base2 as (
    select a.acc_num from account a
)
select first_name, last_name, acc_num
from base
cross join base2;