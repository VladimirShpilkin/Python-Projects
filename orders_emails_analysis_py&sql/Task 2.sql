-- SQLite

SELECT 
    entity,
    FIRST_VALUE(product_family_group) OVER (PARTITION BY entity ORDER BY order_date DESC) as last_product_purchased, 
    order_date
FROM orders

order by order_date asc
