from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator

AIRBYTE_CONNECTION_ID = "ba78537a-fd02-4e98-8a8f-51f91672eee8"

with DAG(
    dag_id="airbyte_connection_test_dag",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["airbyte", "test"],
) as dag:
    start_task = BashOperator(
        task_id="start_airbyte_test",
        bash_command="echo 'Starting Airbyte connection test...'",
    )
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync_job",
        airbyte_conn_id="airbyte_default",  
        connection_id=AIRBYTE_CONNECTION_ID,
        asynchronous=False, 
        timeout=3600,      
    )

    end_task = BashOperator(
        task_id="end_airbyte_test",
        bash_command="echo 'Airbyte connection test complete!'",
    )

    start_task >> trigger_airbyte_sync >> end_task
