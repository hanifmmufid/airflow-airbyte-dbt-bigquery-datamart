with source as (
    SELECT p.product_name, 
    count(whod.quantity) count_qty  
    FROM {{ref('wh_order_details')}} whod
    INNER JOIN {{ref('wh_orders')}} who
    ON whod.order_id = who.id
    INNER JOIN {{ref('wh_products')}} p
    ON p.id = whod.product_id
    GROUP BY p.product_name
    HAVING count(whod.quantity) > 3
    ORDER BY count_qty desc
)

SELECT *, current_timestamp() as ingestion_timestamp 
FROM source