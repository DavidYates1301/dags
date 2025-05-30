from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import List

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc"
NDC_VUNGTAPKET_BCA = "ndc_vungtapket_bca"
NDA_VUNGTAPKET_DANHMUC = "ndc_vungtapket_danhmuc"
DEST_SCHEMA = "ndc_vungdungchung_dancu"
VUNGDUNGCHUNG_DANHMUC = "ndc_vungdungchung_danhmuc"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}

def get_partitions_last_digit() -> List[str]:
    return [str(i) for i in range(10)]

@task
def create_table_if_not_exists(table_name: str, source_schema: str, dest_schema: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{dest_schema}.{table_name} AS
    SELECT * FROM {CATALOG}.{source_schema}.{table_name} WHERE 1=0
    """
    hook.run(sql)

@task
def copy_partition(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)

    condition = f"""
    substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'
    """

    sql = f"""
    INSERT INTO {CATALOG}.{dest_schema}.{table}
    SELECT * FROM {CATALOG}.{source_schema}.{table}
    WHERE {condition}
    ORDER BY {partition_field}
    """
    hook.run(sql)

@task
def copy_full_table(table: str, source_schema: str, dest_schema: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    INSERT INTO {CATALOG}.{dest_schema}.{table}
    SELECT * FROM {CATALOG}.{source_schema}.{table}
    """
    hook.run(sql)

with DAG(
    dag_id="trino_copy_all_tables_partitioned",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=10,
    tags=["trino", "data-copy"]
) as dag:

    # ========== BẢNG PHÂN MẢNH ==========
    partitioned_tables = {
        "diachi": ("matinh", NDC_VUNGTAPKET_BCA),
        "giaytodinhdanhcn": ("sogiayto", NDC_VUNGTAPKET_BCA),
        "nguoivn": ("sodinhdanh", NDC_VUNGTAPKET_BCA)
    }

    for table, (partition_field, source_schema) in partitioned_tables.items():
        create = create_table_if_not_exists.override(task_id=f"create_{table}")(table, source_schema, DEST_SCHEMA)

        for digit in get_partitions_last_digit():
            with TaskGroup(group_id=f"{table}_partition_{digit}") as tg:
                copy_task = copy_partition.override(task_id=f"copy_{table}_{digit}")(
                    table, source_schema, DEST_SCHEMA, partition_field, digit
                )
                create >> copy_task  # đảm bảo task copy phụ thuộc vào task create

    # ========== BẢNG KHÔNG PHÂN MẢNH ==========
    no_partition_tables = [
        ("dm_dantoc", NDA_VUNGTAPKET_DANHMUC),
        ("dm_giatrithithuc", NDA_VUNGTAPKET_DANHMUC),
        ("dm_gioitinh", NDA_VUNGTAPKET_DANHMUC),
        ("dm_huyen", NDA_VUNGTAPKET_DANHMUC),
        ("dm_loaigiaytotuythan", NDA_VUNGTAPKET_DANHMUC),
        ("dm_loaigiaytoxnc", NDA_VUNGTAPKET_DANHMUC),
        ("dm_nhommau", NDA_VUNGTAPKET_DANHMUC),
        ("dm_quoctich", NDA_VUNGTAPKET_DANHMUC),
        ("dm_tinh", NDA_VUNGTAPKET_DANHMUC),
        ("dm_tongiao", NDA_VUNGTAPKET_DANHMUC),
        ("dm_xa", NDA_VUNGTAPKET_DANHMUC),
    ]

    for table, source_schema in no_partition_tables:
        create = create_table_if_not_exists.override(task_id=f"create_{table}")(table, source_schema, VUNGDUNGCHUNG_DANHMUC)
        copy = copy_full_table.override(task_id=f"copy_{table}")(table, source_schema, VUNGDUNGCHUNG_DANHMUC)
        create >> copy
