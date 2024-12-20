with source as (
    select *
    from {{ source('dbt_airflow','raw_suppliers') }}
)
select 
    *,
    current_timestamp() as ingestion_timestamp
from source