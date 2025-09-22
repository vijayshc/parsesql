create table test as
with customers1 as (
    select first_name,last_name from customers2
    inner join (select firstnm,lastnm from customers3)
    on 1=2
)
select
concat(p.first_name, ' ', p.last_name) as full_name
from customers1 p;