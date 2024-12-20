with source as (
    select * from {{ source('dbt_airflow','raw_inventory_transactions') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source