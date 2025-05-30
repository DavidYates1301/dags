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

def get_columns(hook: TrinoHook, schema: str, table: str) -> List[str]:
    sql = f"""
    SELECT column_name
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    results = hook.get_records(sql)
    return [row[0] for row in results]

def generate_merge_sql(hook: TrinoHook, source_schema: str, dest_schema: str, table: str, key: str, condition: str = None) -> str:
    columns = get_columns(hook, source_schema, table)
    escaped_columns = [f'"{col}"' for col in columns]
    columns_str = ", ".join(escaped_columns)
    update_set = ", ".join([f'{col} = source.{col}' for col in escaped_columns if col.strip('"') != key])
    insert_values = ", ".join([f"source.{col}" for col in escaped_columns])
    where_clause = f"WHERE {condition}" if condition else ""

    return f"""
    MERGE INTO {CATALOG}.{dest_schema}.{table} AS target
    USING (
        SELECT * FROM {CATALOG}.{source_schema}.{table} {where_clause}
    ) AS source
    ON target."{key}" = source."{key}"
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({columns_str}) VALUES ({insert_values})
    """

@task
def create_table_if_not_exists(table_name: str, source_schema: str, dest_schema: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{dest_schema}.{table_name} AS
    SELECT * FROM {CATALOG}.{source_schema}.{table_name} WHERE 1=0
    """
    hook.run(sql)

@task
def merge_partition(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str, key: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    condition = f"substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'"
    sql = generate_merge_sql(hook, source_schema, dest_schema, table, key, condition)
    hook.run(sql)

@task
def merge_full_table(table: str, source_schema: str, dest_schema: str, key: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = generate_merge_sql(hook, source_schema, dest_schema, table, key)
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
        "diachi": ("matinh", NDC_VUNGTAPKET_BCA, "madddiadiem"),
        "giaytodinhdanhcn": ("sogiayto", NDC_VUNGTAPKET_BCA, "sogiayto"),
        "nguoivn": ("sodinhdanh", NDC_VUNGTAPKET_BCA, "sodinhdanh"),
    }

    for table, (partition_field, source_schema, key) in partitioned_tables.items():
        create = create_table_if_not_exists.override(task_id=f"create_{table}")(table, source_schema, DEST_SCHEMA)

        for digit in get_partitions_last_digit():
            with TaskGroup(group_id=f"{table}_partition_{digit}") as tg:
                merge_task = merge_partition.override(task_id=f"merge_{table}_{digit}")(
                    table, source_schema, DEST_SCHEMA, partition_field, digit, key
                )
                create >> merge_task

    no_partition_tables = [
        ("dm_dantoc", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_giatrithithuc", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_gioitinh", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_huyen", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_loaigiaytotuythan", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_loaigiaytoxnc", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_nhommau", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_quoctich", NDA_VUNGTAPKET_DANHMUC, "maquocgia"),
        ("dm_tinh", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_tongiao", NDA_VUNGTAPKET_DANHMUC, "ma"),
        ("dm_xa", NDA_VUNGTAPKET_DANHMUC, "ma"),
    ]

    for table, source_schema, key in no_partition_tables:
        create = create_table_if_not_exists.override(task_id=f"create_{table}")(table, source_schema, VUNGDUNGCHUNG_DANHMUC)
        merge = merge_full_table.override(task_id=f"merge_{table}")(table, source_schema, VUNGDUNGCHUNG_DANHMUC, key)
        create >> merge
