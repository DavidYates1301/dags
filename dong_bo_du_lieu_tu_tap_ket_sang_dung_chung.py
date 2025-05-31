from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import List
import logging


SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc" 
NDC_VUNGTAPKET_BCA = "ndc-khotapket-bca"
NDA_VUNGTAPKET_DANHMUC = "ndc-khotapket-danhmuc"
DEST_SCHEMA_DAN_CU = "ndc-khodieuphoi-dancu" 
DEST_SCHEMA_DANH_MUC = "ndc-khodieuphoi-danhmuc" 

default_args = {
    "owner": "nd",
    "depends_on_past": False,
    "retries": 2, 
    "retry_delay": timedelta(seconds=6), 
}

def get_partitions_last_digit() -> List[str]:
    return [str(i) for i in range(10)]

def get_columns(logger: logging.Logger, hook: TrinoHook, schema: str, table: str) -> List[str]:
    sql = f"""
    SELECT column_name
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    logger.info(f"Fetching columns for {CATALOG}.{schema}.{table}")
    results = hook.get_records(sql)
    columns = [row[0] for row in results]
    if not columns:
        logger.warning(f"No columns found for table {CATALOG}.{schema}.{table}. This might indicate a table does not exist or has no defined columns.")
    logger.info(f"Columns for {schema}.{table}: {columns}")
    return columns

def generate_merge_sql(logger: logging.Logger, hook: TrinoHook, source_schema: str, dest_schema: str, table: str, key: str, condition: str = None) -> str:
    columns = get_columns(logger, hook, source_schema, table)
    
    escaped_columns = [f'"{col}"' for col in columns]
    
    columns_str = ", ".join(escaped_columns)
    
    insert_values_str = ", ".join([f"source.{col}" for col in escaped_columns])
    updateable_columns = [col for col in escaped_columns if col.strip('"') != key]
    update_set_clause = ", ".join([f'{col} = source.{col}' for col in updateable_columns])

    where_clause = f"WHERE {condition}" if condition else ""

    merge_sql_parts = [f"""
    MERGE INTO {CATALOG}."{dest_schema}"."{table}" AS target
    USING (
        SELECT {columns_str} FROM {CATALOG}."{source_schema}"."{table}" {where_clause}
    ) AS source
    ON target."{key}" = source."{key}"
    """]

    if update_set_clause:
        merge_sql_parts.append(f"""
    WHEN MATCHED THEN UPDATE SET {update_set_clause}
        """)
    else:
        logger.warning(f"Table '{table}' has no updateable columns besides the key '{key}'. "
                       f"The MERGE statement will only include 'WHEN NOT MATCHED THEN INSERT'.")

    merge_sql_parts.append(f"""
    WHEN NOT MATCHED THEN INSERT ({columns_str}) VALUES ({insert_values_str})
    """)
    
    final_merge_sql = "".join(merge_sql_parts)
    logger.info(f"Generated MERGE SQL for {table} (partition: {condition if condition else 'full'}): \n{final_merge_sql}")
    return final_merge_sql

@task
def create_table_if_not_exists(table_name: str, source_schema: str, dest_schema: str):
    task_log = logging.getLogger(__name__)
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}."{dest_schema}"."{table_name}" AS
    SELECT * FROM {CATALOG}."{source_schema}"."{table_name}" WHERE 1=0
    """
    task_log.info(f"Attempting to create table {CATALOG}.{dest_schema}.{table_name} if not exists.")
    task_log.info(f"Running statement: \n{sql}") 
    try:
        hook.run(sql)
        task_log.info(f"Table {CATALOG}.{dest_schema}.{table_name} creation check completed successfully.")
        return {"create_sql": sql}
    except Exception as e:
        task_log.error(f"Failed to create table {CATALOG}.{dest_schema}.{table_name}: {e}")
        raise 

@task
def merge_partition(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str, key: str):

    task_log = logging.getLogger(__name__)
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    condition = f"substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'"
    task_log.info(f"Starting merge for partitioned table {table}, partition {last_digit}.")
    
    sql = generate_merge_sql(task_log, hook, source_schema, dest_schema, table, key, condition)
    
    try:
        task_log.info(f"Executing MERGE for {table}, partition {last_digit}.")
        hook.run(sql)
        task_log.info(f"MERGE completed for partitioned table {table}, partition {last_digit}.")
        return {"merge_sql": sql}
    except Exception as e:
        task_log.error(f"Failed to merge partition {last_digit} for table {table}: {e}")
        raise

@task
def merge_full_table(table: str, source_schema: str, dest_schema: str, key: str):
    task_log = logging.getLogger(__name__)
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    task_log.info(f"Starting full merge for table {table}.")
    
    sql = generate_merge_sql(task_log, hook, source_schema, dest_schema, table, key)
    
    try:
        task_log.info(f"Executing full MERGE for table {table}.")
        hook.run(sql)
        task_log.info(f"Full MERGE completed for table {table}.")
        return {"merge_sql": sql}
    except Exception as e:
        task_log.error(f"Failed to merge full table {table}: {e}")
        raise

with DAG(
    dag_id="dong_bo_du_lieu_tu_tap_ket_sang_dung_chung", 
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    tags=["trino", "data-merge", "debug"] 
) as dag:

    # ======== Cấu hình BẢNG PHÂN MẢNH (Partitioned Tables) ========
    partitioned_tables = {
        "diachi": ("matinh", NDC_VUNGTAPKET_BCA, "madddiadiem"),
        "giaytodinhdanhcn": ("sogiayto", NDC_VUNGTAPKET_BCA, "sogiayto"),
        "nguoivn": ("sodinhdanh", NDC_VUNGTAPKET_BCA, "sodinhdanh"),
    }

    for table, (partition_field, source_schema, key) in partitioned_tables.items():
        create_task = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(
            table_name=table, source_schema=source_schema, dest_schema=DEST_SCHEMA_DAN_CU
        )

        with TaskGroup(group_id=f"{table}_sync_partitions") as tg_partitions:
            for digit in get_partitions_last_digit():
                merge_task = merge_partition.override(task_id=f"dong_bo_{table}_{digit}")(
                    table=table,
                    source_schema=source_schema,
                    dest_schema=DEST_SCHEMA_DAN_CU,
                    partition_field=partition_field,
                    last_digit=digit,
                    key=key
                )
        create_task >> tg_partitions

    # ======== Cấu hình BẢNG KHÔNG PHÂN MẢNH (Non-Partitioned Tables) ========
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
        create_task = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(
            table_name=table, source_schema=source_schema, dest_schema=DEST_SCHEMA_DANH_MUC
        )
        merge_task = merge_full_table.override(task_id=f"dong_bo_{table}")(
            table=table,
            source_schema=source_schema,
            dest_schema=DEST_SCHEMA_DANH_MUC,
            key=key
        )
        create_task >> merge_task
