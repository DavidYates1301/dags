from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from datetime import datetime, timedelta

SOURCE_CONN_ID = "trino_source"
TARGET_CONN_ID = "trino_target"
SOURCE_SCHEMA = "source_schema"
TARGET_SCHEMA = "target_schema"
TABLE_NAME = "big_table"
BATCH_SIZE = 100000  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@task
def get_total_records():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"SELECT count(*) FROM {SOURCE_SCHEMA}.{TABLE_NAME}"
    result = hook.get_first(sql)
    return result[0] if result else 0

@task
def copy_batch(offset: int, limit: int):
    source_hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    target_hook = TrinoHook(trino_conn_id=TARGET_CONN_ID)

    sql_select = f"""
        SELECT * FROM {SOURCE_SCHEMA}.{TABLE_NAME}
        LIMIT {limit} OFFSET {offset}
    """
    records = source_hook.get_records(sql_select)

    if not records:
        print(f"No data found for offset {offset}")
        return

    for row in records:
        placeholders = ", ".join(["%s"] * len(row))
        sql_insert = f"""
            INSERT INTO {TARGET_SCHEMA}.{TABLE_NAME} VALUES ({placeholders})
        """
        target_hook.run(sql_insert, parameters=row)

with DAG(
    dag_id="trino_distributed_data_copy_batch",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=10,
) as dag:

    total_records = get_total_records()

    batch_ranges = []
    for offset in range(0, 1000000000, BATCH_SIZE):  
        batch_ranges.append(offset)

    copy_tasks = copy_batch.expand(
        offset=batch_ranges,
        limit=[BATCH_SIZE] * len(batch_ranges),
    )
