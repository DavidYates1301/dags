from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from datetime import datetime, timedelta

SOURCE_CONN_ID = "trino_default"
DATABASE = "ndc-common-zone-masterdata"  # Thay bằng tên database chứa 2 schema
SOURCE_SCHEMA = "masterdata"
DEST_SCHEMA = "destination"
TABLE_NAME = "giay_to_dinh_danh_cn"
PARTITION_FIELD = "loaigiayto"
PARTITION_VALUES = [str(i) for i in range(1, 9)]  # ['1','2',...,'8']

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

@task
def create_destination_table():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {DEST_SCHEMA}.{TABLE_NAME} AS
    SELECT * FROM {SOURCE_SCHEMA}.{TABLE_NAME} WHERE 1=0
    """
    hook.run(sql)
    print(f"Table {DEST_SCHEMA}.{TABLE_NAME} created or already exists.")

@task
def copy_partition(loaigiayto_value: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)

    sql_insert = f"""
    INSERT INTO {DEST_SCHEMA}.{TABLE_NAME}
    SELECT * FROM {SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto_value}'
    """
    hook.run(sql_insert)
    print(f"Copied partition loaigiayto={loaigiayto_value} from {SOURCE_SCHEMA} to {DEST_SCHEMA}.")

with DAG(
    dag_id="trino_copy_partitioned_by_loaigiayto_no_delete",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=8,
) as dag:

    create_table = create_destination_table()

    copy_tasks = copy_partition.expand(loaigiayto_value=PARTITION_VALUES)

    create_table >> copy_tasks
