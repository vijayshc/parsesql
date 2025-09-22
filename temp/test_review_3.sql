
select c.first_name, o.total_amount
from customers c
join orders o on c.customer_id = o.customer_id;
