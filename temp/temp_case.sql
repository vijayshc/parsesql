
    CREATE TABLE t_ctas_star AS SELECT * FROM orders;
    SELECT customer_id, order_id, total_amount FROM t_ctas_star;
    