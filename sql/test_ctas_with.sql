create table test as
with cte as (

select concat(first_name1, ' ', last_name2)  as test from 
(
    select * from customers1
) t1

)

select first_name,last_name,first_name1,last_name2 from cte
inner join customers 
on 1=2;


select first_name,last_name,first_name1,last_name2 from 
customers a
inner join
(
    select * from (select first_name1,last_name2 from customers1) t2
) t1
on 1=2;