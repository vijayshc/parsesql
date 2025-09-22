create table test as
with cte as (
    select sno, entity_name from (select * from table1) t1
)
select distinct
t1.sno, t1.entity_name, entitity 
from cte t1
inner join (
    select entitity from table2
) t2
on 1=2;