create table test1
as
with base as (
    select c.first_name,c.last_name from customer c
),
with base2 as (
    select a.acc_num from account a
)
select
first_name, last_name, acc_num
from base a
left outer join base2 c;