
insert into customers 
select o.customer_id, 'New' as first_name, 'Updated' as last_name, 'active' as status
from orders o;
