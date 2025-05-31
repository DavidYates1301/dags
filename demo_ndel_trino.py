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
        connection_id='4ee64da0-cc4f-48e7-8799-e8212bfe6d72', 
        asynchronous=False                                    
    )
    trigger_sync_task2 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_2',
        airbyte_conn_id='ndel_default',              
        connection_id='9db49b60-b174-4eab-b792-51183e2f97e0', 
        asynchronous=False                                    
    )
    trigger_sync_task3 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_3',
        airbyte_conn_id='ndel_default',              
        connection_id='1fdd03c1-8aab-48b1-b4af-dbe8d5ac2a89', 
        asynchronous=False                                    
    )
    trigger_sync_task4 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_4',
        airbyte_conn_id='ndel_default',              
        connection_id='e41d6b4a-5121-4bfd-bd11-1e6bcffd8926', 
        asynchronous=False                                    
    )
    trigger_sync_task5 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_5',
        airbyte_conn_id='ndel_default',              
        connection_id='2a678800-2965-4af5-b1af-a302a5ef71ca', 
        asynchronous=False                                    
    )
    trigger_sync_task6 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_6',
        airbyte_conn_id='ndel_default',              
        connection_id='1d5b7a5d-73b1-4e60-87e9-cf4d1681883f', 
        asynchronous=False                                    
    )
    trigger_sync_task7 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_7',
        airbyte_conn_id='ndel_default',              
        connection_id='a057a293-603a-4a60-ad37-c57030f605e0', 
        asynchronous=False                                    
    )
    trigger_sync_task8 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_8',
        airbyte_conn_id='ndel_default',              
        connection_id='eb6d97ae-e8cd-4d24-9920-4c1f2754f35f', 
        asynchronous=False                                    
    )
    trigger_sync_task9 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_9',
        airbyte_conn_id='ndel_default',              
        connection_id='f33a75a9-66dc-4a13-b167-db60254384bb', 
        asynchronous=False                                    
    )
    trigger_sync_task10 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_10',
        airbyte_conn_id='ndel_default',              
        connection_id='aa4d0703-a092-4c34-9c73-2ae205a05226', 
        asynchronous=False                                    
    )
    trigger_sync_task11 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_11',
        airbyte_conn_id='ndel_default',              
        connection_id='7eafe181-193c-4553-9267-a6b63bd5940b', 
        asynchronous=False                                    
    )
    trigger_sync_task12 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_12',
        airbyte_conn_id='ndel_default',              
        connection_id='4fd46cff-a316-45cc-bb8a-242b5ffe1620', 
        asynchronous=False                                    
    )
    trigger_sync_task13 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_13',
        airbyte_conn_id='ndel_default',              
        connection_id='59b1b38f-5107-452c-808e-24de13f4458f', 
        asynchronous=False                                    
    )

    trigger_sync_task14 = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte_sync_14',
        airbyte_conn_id='ndel_default',              
        connection_id='4c4c3d0f-34bc-4951-9d4c-5e0f9549e9e5', 
        asynchronous=False                                    
    )
    trigger_sync_task1 >>  trigger_sync_task2 >> trigger_sync_task3 >> trigger_sync_task4 >> trigger_sync_task5 >>  trigger_sync_task6 >> trigger_sync_task7 >> trigger_sync_task8 >> trigger_sync_task9 >>  trigger_sync_task10 >> trigger_sync_task11 >> trigger_sync_task12 >> trigger_sync_task13 >> trigger_sync_task14

    trigger_trino_dag = TriggerDagRunOperator(
        task_id='trigger_trino_copy_all_tables_partitioned',
        # trigger_dag_id='trino_copy_all_tables_partitioned',  
        trigger_dag_id='trino_merge_all_tables_partitioned_v2',
        wait_for_completion=False,  
        reset_dag_run=True,         
    )

    trigger_sync_task14 >> trigger_trino_dag
