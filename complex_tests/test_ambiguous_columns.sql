-- Complex Test 6: Test ambiguous column resolution priorities
select customer_id, order_total, special_note
from customer_data c
join order_summary o on c.customer_id = o.customer_id  
join (select customer_id, priority_note as special_note from priority_table) p on c.customer_id = p.customer_id
join (select customer_id, standard_note as special_note from standard_table) s on c.customer_id = s.customer_id;