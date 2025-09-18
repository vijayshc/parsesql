WITH orders_base AS (
  SELECT order_id, customer_id, total_amount FROM orders
), cust_base AS (
  SELECT customer_id, first_name FROM customers
), joined AS (
  SELECT o.order_id, o.customer_id, c.first_name FROM orders_base o JOIN cust_base c ON o.customer_id = c.customer_id
)
SELECT * FROM joined;