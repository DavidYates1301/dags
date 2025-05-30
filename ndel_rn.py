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
ORDER_COLUMN = "sogiayto"
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
    print(f"Table {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME} is ready.")

@task
def get_total_batches():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"SELECT COUNT(*) FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}"
    count = hook.get_first(sql)[0]
    print(f"Total rows in source table: {count}")

    task_inputs = []
    num_batches = math.ceil(count / BATCH_SIZE)
    for i in range(num_batches):
        start_row = i * BATCH_SIZE + 1
        end_row = min((i + 1) * BATCH_SIZE, count)  # tránh end_row vượt quá tổng số dòng
        task_inputs.append({
            "start_row": start_row,
            "end_row": end_row
        })
    return task_inputs

@task
def copy_batch(batch):
    start_row = batch["start_row"]
    end_row = batch["end_row"]

    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    INSERT INTO {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME}
    SELECT loaigiayto, tengiayto, ngaycap, noicap, sogiayto, "_class"
    FROM (
        SELECT *, row_number() OVER (ORDER BY {ORDER_COLUMN}) AS rn
        FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    ) t
    WHERE t.rn BETWEEN {start_row} AND {end_row}
    """
    hook.run(sql)
    print(f"Copied rows from {start_row} to {end_row}")

with DAG(
    dag_id="trino_copy_whole_table_in_batches_v2",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1,
) as dag:

    create = create_destination_table()
    batches = get_total_batches()
    copy_tasks = copy_batch.expand(batch=batches)

    create >> batches >> copy_tasks
