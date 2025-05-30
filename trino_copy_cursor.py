from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import List, Dict

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc_common_zone_masterdata"
SOURCE_SCHEMA = "masterdata"
DEST_SCHEMA = "destination"
TABLE_NAME = "giay_to_dinh_danh_cn"
PARTITION_FIELD = "loaigiayto"
ORDER_FIELD = "sogiayto"  # Dùng như cursor
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
def get_cursor_batches(loaigiayto: str) -> List[Dict]:
    """
    Lấy danh sách batch theo cursor (sogiayto) cho mỗi partition.
    Trả về danh sách dict chứa start_cursor, end_cursor
    """
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)

    # Lấy toàn bộ danh sách cursor (sogiayto) đã sắp xếp
    sql = f"""
    SELECT {ORDER_FIELD} FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto}'
    ORDER BY {ORDER_FIELD}
    """
    rows = hook.get_pandas_df(sql)[ORDER_FIELD].tolist()

    if not rows:
        return []

    batches = []
    for i in range(0, len(rows), BATCH_SIZE):
        batch_rows = rows[i:i + BATCH_SIZE]
        start_cursor = batch_rows[0] if i > 0 else ""  # start = "" nếu là batch đầu
        end_cursor = batch_rows[-1]
        batches.append({
            "loaigiayto": loaigiayto,
            "start_cursor": start_cursor,
            "end_cursor": end_cursor
        })

    print(f"Partition {loaigiayto}: generated {len(batches)} batches")
    return batches


@task
def copy_batch_by_cursor(batch: Dict):
    """
    Thực hiện copy 1 batch theo range của cursor
    """
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    loaigiayto = batch["loaigiayto"]
    start_cursor = batch["start_cursor"]
    end_cursor = batch["end_cursor"]

    condition = f"""
        {PARTITION_FIELD} = '{loaigiayto}' AND
        {ORDER_FIELD} <= '{end_cursor}'
    """
    if start_cursor:
        condition += f" AND {ORDER_FIELD} > '{start_cursor}'"

    sql = f"""
    INSERT INTO {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME}
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {condition}
    ORDER BY {ORDER_FIELD}
    """
    hook.run(sql)
    print(f"Copied partition {loaigiayto} from '{start_cursor}' to '{end_cursor}'")


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
            batches = get_cursor_batches.override(task_id=f"get_batches_{value}")(value)
            copy_batch_by_cursor.override(task_id=f"copy_batch_{value}").expand(batch=batches)

        create_table >> partition_group
