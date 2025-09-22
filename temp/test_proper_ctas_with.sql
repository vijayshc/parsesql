create table test as
with base as (
    select concat(first_name, ' ', last_name) as test from customers1
),
base2 as (
    select * from base
)
select * from base2;