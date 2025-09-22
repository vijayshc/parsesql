create table test as
with base as (
    select * from customers
)
select * from base;
