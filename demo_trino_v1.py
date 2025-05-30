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
def merge_partition(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str, unique_field: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    condition = f"substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'"
    
    sql = f"""
    MERGE INTO {CATALOG}.{dest_schema}.{table} AS target
    USING (
        SELECT * FROM {CATALOG}.{source_schema}.{table} WHERE {condition}
    ) AS source
    ON target.{unique_field} = source.{unique_field}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;
    """
    hook.run(sql)

@task
def merge_full_table(table: str, source_schema: str, dest_schema: str, unique_field: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    MERGE INTO {CATALOG}.{dest_schema}.{table} AS target
    USING {CATALOG}.{source_schema}.{table} AS source
    ON target.{unique_field} = source.{unique_field}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *;
    """
    hook.run(sql)

with DAG(
    dag_id="trino_merge_all_tables_partitioned",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=10,
    tags=["trino", "data-merge"]
) as dag:

    partitioned_tables = {
        "diachi": ("matinh", "madiadiem", NDC_VUNGTAPKET_BCA),
        "giaytodinhdanhcn": ("sogiayto", "sogiayto", NDC_VUNGTAPKET_BCA),
        "nguoivn": ("sodinhdanh", "sodinhdanh", NDC_VUNGTAPKET_BCA),
    }

    for table, (partition_field, unique_field, source_schema) in partitioned_tables.items():
        create = create_table_if_not_exists.override(task_id=f"create_{table}")(table, source_schema, DEST_SCHEMA)

        for digit in get_partitions_last_digit():
            with TaskGroup(group_id=f"{table}_partition_{digit}") as tg:
                merge_task = merge_partition.override(task_id=f"merge_{table}_{digit}")(
                    table, source_schema, DEST_SCHEMA, partition_field, digit, unique_field
                )
                create >> merge_task

    no_partition_tables = [
        ("dm_dantoc", "ma"),
        ("dm_giatrithithuc", "ma"),
        ("dm_gioitinh", "ma"),
        ("dm_huyen", "ma"),
        ("dm_loaigiaytotuythan", "ma"),
        ("dm_loaigiaytoxnc", "ma"),
        ("dm_nhommau", "ma"),
        ("dm_quoctich", "maquocgia"),
        ("dm_tinh", "ma"),
        ("dm_tongiao", "ma"),
        ("dm_xa", "ma"),
    ]

    for table, unique_field in no_partition_tables:
        create = create_table_if_not_exists.override(task_id=f"create_{table}")(table, NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC)
        merge = merge_full_table.override(task_id=f"merge_{table}")(table, NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, unique_field)
        create >> merge
