from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import List
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "ND",
    "depends_on_past": False,
}

with DAG(
    dag_id='dong_bo_du_lieu_ndel_v1',
    start_date=datetime(2023, 1, 1),
    default_args=default_args,
    schedule=None,  
    catchup=False
) as dag:
    trigger_sync_task1 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_danh_muc_dan_cu',
        airbyte_conn_id='ndel_default',              
        connection_id='4499fa55-5cae-45a8-bcb4-d07b7427822f', 
        asynchronous=False                                    
    )
    trigger_sync_task2 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_dia_chi',
        airbyte_conn_id='ndel_default',              
        connection_id='29ecbcbc-8744-4ba3-b78d-9aad14481d34', 
        asynchronous=False                                    
    )
    trigger_sync_task3 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_giay_to_dinh_danh_ca_nhan',
        airbyte_conn_id='ndel_default',              
        connection_id='5cca7752-09c8-48d5-8104-d386b9ae5385', 
        asynchronous=False                                    
    )

    trigger_sync_task4 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_cong_dan',
        airbyte_conn_id='ndel_default',              
        connection_id='e20cd1ef-f739-43cd-b1e3-d1233dc11fb4', 
        asynchronous=False                                    
    )

    trigger_sync_task5 = TriggerDagRunOperator(
        task_id='dong_bo_vung_tap_ket_sang_vung_dieu_phoi',
        trigger_dag_id='dong_bo_du_lieu_tu_tap_ket_sang_dung_chung',
        wait_for_completion=False,  
        reset_dag_run=True,         
    )

    trigger_sync_task1 >>  trigger_sync_task2 >> trigger_sync_task3 >> trigger_sync_task4 >> trigger_sync_task5
