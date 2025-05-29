from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

SOURCE_CONN_ID = "trino_source"
TARGET_CONN_ID = "trino_target"
SOURCE_SCHEMA = "source_schema"
TARGET_SCHEMA = "target_schema"
TABLE_NAME = "big_table"
PARTITION_COLUMN = "partition_key"  
PARTITIONS = ["2023-01", "2023-02", "2023-03", "2023-04"]  
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

def copy_partition(partition):
    source_hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    target_hook = TrinoHook(trino_conn_id=TARGET_CONN_ID)

    sql_select = f"""
        SELECT * FROM {SOURCE_SCHEMA}.{TABLE_NAME}
        WHERE {PARTITION_COLUMN} = '{partition}'
    """
    records = source_hook.get_records(sql_select)

    if not records:
        print(f"No data found for partition {partition}")
        return

    sql_delete = f"""
        DELETE FROM {TARGET_SCHEMA}.{TABLE_NAME}
        WHERE {PARTITION_COLUMN} = '{partition}'
    """
    target_hook.run(sql_delete)

    for row in records:
        placeholders = ", ".join(["%s"] * len(row))
        sql_insert = f"""
            INSERT INTO {TARGET_SCHEMA}.{TABLE_NAME} VALUES ({placeholders})
        """
        target_hook.run(sql_insert, parameters=row)

with DAG(
    dag_id="trino_distributed_data_copy",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_tasks=10,
) as dag:

    with TaskGroup("copy_partitions") as copy_partitions:
        for partition in PARTITIONS:
            PythonOperator(
                task_id=f"copy_partition_{partition.replace('-', '_')}",
                python_callable=copy_partition,
                op_args=[partition],
            )
