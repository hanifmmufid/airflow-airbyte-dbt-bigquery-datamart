# di file operators/custom_airbyte_operator.py

from airflow.models import BaseOperator
import requests

class CustomAirbyteOperator(BaseOperator):
    def __init__(
        self,
        connection_id: str,
        airbyte_url: str = "http://localhost:8000",
        api_key: str = None,  # Tambahkan parameter untuk API key
        **kwargs
    ):
        super().__init__(**kwargs)
        self.connection_id = connection_id
        self.airbyte_url = airbyte_url
        self.api_key = api_key

    def execute(self, context):
        sync_endpoint = f"{self.airbyte_url}/api/v1/connections/sync"
        
        # Tambahkan headers untuk autentikasi
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        self.log.info(f"Memulai sinkronisasi untuk connection_id: {self.connection_id}")
        
        response = requests.post(
            sync_endpoint,
            headers=headers,
            json={"connectionId": self.connection_id}
        )
        
        if response.status_code != 200:
            raise Exception(f"Sinkronisasi gagal: {response.text}")
            
        return response.json()