create table test as
with customers1 as (
    select cif,a.customer_id from customer
    inner join customer_good a
    on customer.customer_id = a.customer_id
) 
,customer2 as (
    select cif,a.customer_id from customer
    inner join customer_bad a
    on customer.customer_id = a.customer_id
)
,cif as (
    select customer_id from customers1
    UNION
    select customer_id from customer2
)
select 
first_name
, last_name
,a.customer_id,test
from cif a
inner join (
    select test from test1
    union all
    select test1 from test2
)as t1
on cifs.customer_id = a.customer_id;