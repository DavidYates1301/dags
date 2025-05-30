from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from datetime import datetime, timedelta
import math

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc_common_zone_masterdata"
SOURCE_SCHEMA = "masterdata"
DEST_SCHEMA = "destination"
TABLE_NAME = "giay_to_dinh_danh_cn"
PARTITION_FIELD = "loaigiayto"
BATCH_SIZE = 5000

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=3),
}

@task
def create_destination_table():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME} AS
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME} WHERE 1=0
    """
    hook.run(sql)

@task
def get_partition_batches():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    SELECT {PARTITION_FIELD}, COUNT(*) FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    GROUP BY {PARTITION_FIELD}
    """
    records = hook.get_records(sql)

    task_inputs = []
    for part_value, count in records:
        num_batches = math.ceil(count / BATCH_SIZE)
        for i in range(num_batches):
            offset = i * BATCH_SIZE
            task_inputs.append({
                "loaigiayto_value": str(part_value),
                "offset": offset
            })
    return task_inputs

@task
def copy_partition_batch(batch):
    loaigiayto_value = batch["loaigiayto_value"]
    offset = batch["offset"]

    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    INSERT INTO {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME}
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto_value}'
    ORDER BY id
    OFFSET {offset} LIMIT {BATCH_SIZE}
    """
    hook.run(sql)
    print(f"Copied loaigiayto={loaigiayto_value}, offset={offset}")

with DAG(
    dag_id="trino_copy_partitioned_batches_dynamic",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1,
) as dag:

    create = create_destination_table()
    batches = get_partition_batches()
    copy_tasks = copy_partition_batch.expand(batch=batches)

    create >> batches >> copy_tasks
