create table customer1 
as
with base as (
    select first_name,last_name
    from (
        select * from customers1
    )
),
with base2 as (
    select * from base
)
select * from base2;

select first_id,last_id
from (
    select * from customer1
) t1
;