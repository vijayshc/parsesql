
select t.customer_id, t.name_concat
from (
    select customer_id, concat(first_name, last_name) as name_concat 
    from customers
) t;
