from airflow import DAG
from datetime import datetime, timedelta
from operators.custom_airbyte_operator import CustomAirbyteOperator
from operators.custom_airbyte_operator_v2 import WaitForAirbyteSyncOperator
import os
from dotenv import load_dotenv
from airflow.utils.task_group import TaskGroup
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig    
from pathlib import Path
from datetime import datetime, timedelta
from include.dbt_airflow.cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG

# Load environment variables
load_dotenv()

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="ingest_data_airbyte_v4",
    default_args=default_args,
    start_date=datetime(2024, 11, 10),
    schedule_interval="@daily",
    catchup=False,
    tags=['airbyte', 'airflow', 'bigquery']
) as dag:

    # TaskGroup untuk sinkronisasi CSV ke BigQuery
    with TaskGroup("csv_to_bigquery_syncs", tooltip="Sync CSV to BigQuery") as csv_to_bigquery_group:
        csv_to_bigquery_inventory_transactions = CustomAirbyteOperator(
            task_id='ingest_csv_to_bigquery_inventory_transactions',
            airbyte_url=os.getenv('AIRBYTE_URL'),
            connection_id=os.getenv('AIRBYTE_CONN_CSV_INVENTORY_TRANSACTIONS'),
            api_key=os.getenv('AIRBYTE_API_KEY')
        )

        csv_to_bigquery_order_details = CustomAirbyteOperator(
            task_id='ingest_csv_to_bigquery_order_details',
            airbyte_url=os.getenv('AIRBYTE_URL'),
            connection_id=os.getenv('AIRBYTE_CONN_CSV_ORDER_DETAILS'),
            api_key=os.getenv('AIRBYTE_API_KEY')
        )

        csv_to_bigquery_orders = CustomAirbyteOperator(
            task_id='ingest_csv_to_bigquery_orders',
            airbyte_url=os.getenv('AIRBYTE_URL'),
            connection_id=os.getenv('AIRBYTE_CONN_CSV_ORDERS'),
            api_key=os.getenv('AIRBYTE_API_KEY')
        )

        csv_to_bigquery_products = CustomAirbyteOperator(
            task_id='ingest_csv_to_bigquery_products',
            airbyte_url=os.getenv('AIRBYTE_URL'),
            connection_id=os.getenv('AIRBYTE_CONN_CSV_PRODUCTS'),
            api_key=os.getenv('AIRBYTE_API_KEY')
        )

        csv_to_bigquery_suppliers = CustomAirbyteOperator(
            task_id='ingest_csv_to_bigquery_suppliers',
            airbyte_url=os.getenv('AIRBYTE_URL'),
            connection_id=os.getenv('AIRBYTE_CONN_CSV_SUPPLIERS'),
            api_key=os.getenv('AIRBYTE_API_KEY')
        )

        csv_to_bigquery_inventory_transactions >> csv_to_bigquery_order_details >> csv_to_bigquery_orders >> csv_to_bigquery_products >> csv_to_bigquery_suppliers

    # WaitForAirbyteSyncOperator untuk menunggu semua sinkronisasi selesai
    wait_for_sync = WaitForAirbyteSyncOperator(
        task_id='wait_for_sync',
        airbyte_url=os.getenv("AIRBYTE_URL"),
        connection_ids=[
            os.getenv('AIRBYTE_CONN_CSV_INVENTORY_TRANSACTIONS'),
            os.getenv('AIRBYTE_CONN_CSV_ORDER_DETAILS'),
            os.getenv('AIRBYTE_CONN_CSV_ORDERS'),
            os.getenv('AIRBYTE_CONN_CSV_PRODUCTS'),
            os.getenv('AIRBYTE_CONN_CSV_SUPPLIERS')
        ],
        api_key=os.getenv("AIRBYTE_API_KEY"),
        polling_interval=30,  # Poll setiap 30 detik
        timeout=3600  # Timeout setelah 1 jam
    )

    # Modify the DbtTaskGroup instantiation
    transfrom_data_dbt = DbtTaskGroup(
        group_id="dbtTaskGroup",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models']
        )
    )

    # Dependency chain
    csv_to_bigquery_group >> wait_for_sync >> transfrom_data_dbt
