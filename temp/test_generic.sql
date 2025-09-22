create table test1 as
with base as (
    select concat(first_name, last_name) from customers
)
select * from base;

create table test2 as  
with base as (
    select sum(customer_id), avg(customer_id), substr(first_name, 1, 2) from customers
)
select * from base;

create table test3 as
with base as (
    select first_name + last_name, customer_id * 2, case when status='active' then 1 else 0 end from customers  
)
select * from base;