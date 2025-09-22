with cte as (
    select 'test',concat(first_name1, ' ', last_name2) as test from 
    (
        select * from customers1
    ) t1
)
select 'test', test,first_name,last_name from cte
inner join (select first_name,last_name from customers2) t2 
on 1=2
;