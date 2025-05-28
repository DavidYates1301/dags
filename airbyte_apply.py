from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import datetime, timedelta

# Khởi tạo DAG
with DAG(
    dag_id='airbyte_sync',
    start_date=datetime(2023, 1, 1),
    schedule=timedelta(days=1),  # Chạy thủ công hoặc trigger qua API Airflow
    catchup=False
) as dag:
    # Task 1: Kích hoạt job Airbyte (async)
    trigger_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        airbyte_conn_id='airbyte_http',              # kết nối HTTP đã cấu hình
        connection_id='ba78537a-fd02-4e98-8a8f-51f91672eee8', # ID Connection Airbyte
        asynchronous=True                                    # chỉ trigger, không đợi ở đây
    )
    # Task 2: Sensor theo dõi job Airbyte đến khi hoàn tất
    wait_for_sync = AirbyteJobSensor(
        task_id='wait_for_airbyte_sync',
        airbyte_conn_id='airbyte_http',
        airbyte_job_id=trigger_sync.output                   # lấy job_id từ task trước
    )
    trigger_sync >> wait_for_sync
