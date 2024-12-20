with source as (
    select * from {{ source('dbt_airflow','raw_order_details') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source     