from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import List
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id='airbyte_sync_v2',
    start_date=datetime(2023, 1, 1),
    schedule=None,  
    catchup=False
) as dag:
    trigger_sync_task1 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_1',
        airbyte_conn_id='ndel_default',              
        connection_id='7cfead0a-31de-4ae2-92e2-603eb095a3a4', 
        asynchronous=False                                    
    )
    trigger_sync_task2 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_2',
        airbyte_conn_id='ndel_default',              
        connection_id='4fd46cff-a316-45cc-bb8a-242b5ffe1620', 
        asynchronous=False                                    
    )
    trigger_sync_task3 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_3',
        airbyte_conn_id='ndel_default',              
        connection_id='59b1b38f-5107-452c-808e-24de13f4458f', 
        asynchronous=False                                    
    )

    trigger_sync_task4 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_4',
        airbyte_conn_id='ndel_default',              
        connection_id='4c4c3d0f-34bc-4951-9d4c-5e0f9549e9e5', 
        asynchronous=False                                    
    )
    trigger_sync_task1 >>  trigger_sync_task2 >> trigger_sync_task3 >> trigger_sync_task4

    trigger_trino_dag = TriggerDagRunOperator(
        task_id='trigger_trino_copy_all_tables_partitioned',
        trigger_dag_id='trino_copy_all_tables_partitioned',  
        wait_for_completion=False,  
        reset_dag_run=True,         
    )

    trigger_sync_task4 >> trigger_trino_dag
