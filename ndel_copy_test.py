from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import math

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc_common_zone_masterdata"
SOURCE_SCHEMA = "masterdata"
DEST_SCHEMA = "destination"
TABLE_NAME = "giay_to_dinh_danh_cn"
PARTITION_FIELD = "loaigiayto"
PARTITION_VALUES = [str(i) for i in range(1, 9)]
BATCH_SIZE = 1000

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}

@task
def create_destination_table():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME} AS
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME} WHERE 1=0
    """
    hook.run(sql)
    print(f"Destination table created: {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME}")

@task
def get_batches_for_partition(loaigiayto: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    count_sql = f"""
    SELECT COUNT(*) FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto}'
    """
    count = hook.get_first(count_sql)[0]
    print(f"loaigiayto={loaigiayto} has {count} records")
    num_batches = math.ceil(count / BATCH_SIZE)
    return [
        {
            "loaigiayto": loaigiayto,
            "offset": i * BATCH_SIZE
        }
        for i in range(num_batches)
    ]

@task
def copy_batch(batch: dict):
    loaigiayto = batch["loaigiayto"]
    offset = batch["offset"]
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    INSERT INTO {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME}
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto}'
    ORDER BY sogiayto
    OFFSET {offset}
    LIMIT {BATCH_SIZE}
    """
    hook.run(sql)
    print(f"Copied: loaigiayto={loaigiayto}, offset={offset}")

with DAG(
    dag_id="trino_copy_partitioned_batched_dynamic_new",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,  # hạn chế tải cho Trino
) as dag:

    create_table = create_destination_table()

    for value in PARTITION_VALUES:
        with TaskGroup(group_id=f"partition_{value}") as partition_group:
            batches = get_batches_for_partition.override(task_id=f"get_batches_{value}")(value)
            copy_batch.expand(batch=batches)

        create_table >> partition_group
