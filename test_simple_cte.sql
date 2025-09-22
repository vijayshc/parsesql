with cte as (
    select first_name1, last_name2 from customers1
)
select first_name1, last_name2 from cte;