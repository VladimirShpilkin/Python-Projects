-- SQLite

SELECT distinct(entity) as customer_id,
group_concat(order_id) as customers_orders,
count(order_id) as orders_count,
round(sum(amt_income_gross_total_usd),1) as orders_sum_usd
from orders
group by entity
order by orders_count desc
limit 5;




