create table test as
with cte as (
    select sno, entity_name from (select * from table1) t1
)
select distinct
sno, entity_name, entitity 
from cte t1
left outer join table2 t2
on 1=2
left outer join table3  t3
on 1=2
left outer join (select * from table4) t4;