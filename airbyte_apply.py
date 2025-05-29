from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from datetime import datetime, timedelta

with DAG(
    dag_id='airbyte_sync',
    start_date=datetime(2023, 1, 1),
    schedule=None,  
    catchup=False
) as dag:
    trigger_sync_task1 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        airbyte_conn_id='airbyte_conn',              
        connection_id='ba78537a-fd02-4e98-8a8f-51f91672eee8', 
        asynchronous=False                                    
    )
    trigger_sync_task2 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        airbyte_conn_id='airbyte_conn',              
        connection_id='ea9ee394-f349-48f8-9fe2-393ff1fff9bf', 
        asynchronous=False                                    
    )
    trigger_sync_task3 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync',
        airbyte_conn_id='airbyte_conn',              
        connection_id='21d8b23f-161a-4cfa-b069-c4d1991e4bdc', 
        asynchronous=False                                    
    )
    trigger_sync_task1 >>  trigger_sync_task2 >> trigger_sync_task3
