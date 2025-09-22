-- Test with corrected CTE that includes missing columns
create table test_corrected as
with cte as (
    select first_name1, last_name2, concat(first_name1, ' ', last_name2) as test 
    from (select * from customers1) t1
)
select first_name,last_name,first_name1,last_name2 from cte
inner join customers 
on 1=2;