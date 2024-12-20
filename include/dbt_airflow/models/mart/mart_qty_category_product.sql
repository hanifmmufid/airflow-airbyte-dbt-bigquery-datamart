with source as (
    SELECT p.category, count(whod.quantity) as qty 
    FROM {{ref('wh_order_details')}} whod
    INNER JOIN {{ref('wh_orders')}} who
    ON whod.order_id = who.id
    INNER JOIN {{ref('wh_products')}} p
    ON p.id = whod.product_id
    GROUP BY p.category
    HAVING count(whod.quantity) > 1
    ORDER BY qty desc
)

SELECT *, current_timestamp() as ingestion_timestamp 
FROM source