select * from (
select max(test) from (
select concat(first_name,last_name) as test from base2
) test1
) test2;
