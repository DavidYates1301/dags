from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc_common_zone_masterdata"
SOURCE_SCHEMA = "masterdata"
DEST_SCHEMA = "destination"
TABLE_NAME = "giay_to_dinh_danh_cn"
PARTITION_FIELD = "loaigiayto"
ORDER_FIELD = "sogiayto"  # Cursor field
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

def get_cursor_start(loaigiayto: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    SELECT MIN({ORDER_FIELD}) FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto}'
    """
    start_cursor = hook.get_first(sql)[0]
    return {"loaigiayto": loaigiayto, "cursor": start_cursor}

@task

def get_next_batch(params: dict):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    loaigiayto = params["loaigiayto"]
    last_cursor = params["cursor"]

    sql = f"""
    SELECT {ORDER_FIELD} FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto}' AND {ORDER_FIELD} > '{last_cursor}'
    ORDER BY {ORDER_FIELD}
    LIMIT {BATCH_SIZE}
    """
    rows = hook.get_pandas_df(sql)[ORDER_FIELD].tolist()
    if not rows:
        return []

    new_cursor = rows[-1]
    return [{"loaigiayto": loaigiayto, "start_cursor": last_cursor, "end_cursor": new_cursor}]

@task

def copy_batch_by_cursor(batch: dict):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    loaigiayto = batch["loaigiayto"]
    start_cursor = batch["start_cursor"]
    end_cursor = batch["end_cursor"]

    sql = f"""
    INSERT INTO {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME}
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto}'
      AND {ORDER_FIELD} > '{start_cursor}' AND {ORDER_FIELD} <= '{end_cursor}'
    ORDER BY {ORDER_FIELD}
    """
    hook.run(sql)
    print(f"Copied: loaigiayto={loaigiayto}, {start_cursor} -> {end_cursor}")

with DAG(
    dag_id="trino_copy_partitioned_cursor_based",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
) as dag:

    create_table = create_destination_table()

    for value in PARTITION_VALUES:
        with TaskGroup(group_id=f"partition_{value}") as partition_group:
            cursor_start = get_cursor_start.override(task_id=f"get_start_{value}")(value)
            next_batches = get_next_batch.override(task_id=f"get_batches_{value}")(cursor_start)
            copy_batch_by_cursor.expand(batch=next_batches)

        create_table >> partition_group
