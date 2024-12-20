with source as (
    SELECT p.product_name, 
    sum(whod.quantity * whod.unit_price) as total_price  
    FROM {{ref('wh_order_details')}} whod
    INNER JOIN {{ref('wh_orders')}} who
    ON whod.order_id = who.id
    INNER JOIN {{ref('wh_products')}} p
    ON p.id = whod.product_id
    GROUP BY p.product_name
    HAVING sum(whod.quantity * whod.unit_price) > 1000
    ORDER BY total_price desc
)

SELECT *, current_timestamp() as ingestion_timestamp 
FROM source