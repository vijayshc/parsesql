create table test1 as select * from customers;

select customer_id, order_id, total_amount * 1.1 as adjusted from test1;