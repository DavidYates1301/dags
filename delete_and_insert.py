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
DEST_SCHEMA_DAN_CU = "ndc_vungdungchung_dancu"
DEST_SCHEMA_DANH_MUC = "ndc_vungdungchung_danhmuc"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

def get_partitions_last_digit() -> List[str]:
    """Trả về danh sách các chữ số cuối cùng (0-9) để phân chia."""
    return [str(i) for i in range(10)]

def get_columns(hook: TrinoHook, schema: str, table: str) -> List[str]:
    """
    Lấy danh sách các tên cột của một bảng từ information_schema của Trino.
    Các cột được sắp xếp theo vị trí thứ tự của chúng.
    """
    sql = f"""
    SELECT column_name
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    log.info(f"Fetching columns for {CATALOG}.{schema}.{table}")
    results = hook.get_records(sql)
    columns = [row[0] for row in results]
    if not columns:
        log.warning(f"No columns found for table {CATALOG}.{schema}.{table}. This might indicate a table does not exist or has no defined columns.")
    log.info(f"Columns for {schema}.{table}: {columns}")
    return columns

def generate_delete_insert_sqls(hook: TrinoHook, source_schema: str, dest_schema: str, table: str, key: str, condition: str = None) -> (str, str):

    columns = get_columns(hook, source_schema, table)
    escaped_columns = [f'"{col}"' for col in columns]
    columns_str = ", ".join(escaped_columns)
    
    where_clause = f"WHERE {condition}" if condition else ""


    delete_sql = f"""
    DELETE FROM {CATALOG}.{dest_schema}.{table}
    WHERE "{key}" IN (
        SELECT source."{key}"
        FROM {CATALOG}.{source_schema}.{table} AS source
        {where_clause}
    )
    """

 
    insert_sql = f"""
    INSERT INTO {CATALOG}.{dest_schema}.{table} ({columns_str})
    SELECT {columns_str}
    FROM {CATALOG}.{source_schema}.{table} AS source
    {where_clause}
    """
    
    log.info(f"Generated DELETE SQL for {table} (partition: {condition if condition else 'full'}): \n{delete_sql}")
    log.info(f"Generated INSERT SQL for {table} (partition: {condition if condition else 'full'}): \n{insert_sql}")
    
    return delete_sql, insert_sql

@task
def create_table_if_not_exists(table_name: str, source_schema: str, dest_schema: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{dest_schema}.{table_name} AS
    SELECT * FROM {CATALOG}.{source_schema}.{table_name} WHERE 1=0
    """
    log.info(f"Attempting to create table {CATALOG}.{dest_schema}.{table_name} if not exists.")
    try:
        hook.run(sql)
        log.info(f"Table {CATALOG}.{dest_schema}.{table_name} creation check completed successfully.")
    except Exception as e:
        log.error(f"Failed to create table {CATALOG}.{dest_schema}.{table_name}: {e}")
        raise 

@task
def sync_partition_delete_insert(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str, key: str):
    """
    Thực hiện đồng bộ hóa cho một phân vùng cụ thể của bảng bằng cách DELETE và sau đó INSERT.
    """
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    condition = f"substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'"
    log.info(f"Starting sync (DELETE+INSERT) for partitioned table {table}, partition {last_digit}.")
    
    delete_sql, insert_sql = generate_delete_insert_sqls(hook, source_schema, dest_schema, table, key, condition)
    
    try:
        log.info(f"Executing DELETE for {table}, partition {last_digit}.")
        hook.run(delete_sql)
        log.info(f"Executing INSERT for {table}, partition {last_digit}.")
        hook.run(insert_sql)
        log.info(f"Sync (DELETE+INSERT) completed for partitioned table {table}, partition {last_digit}.")
    except Exception as e:
        log.error(f"Failed to sync partition {last_digit} for table {table} using DELETE+INSERT: {e}")
        raise

@task
def sync_full_table_delete_insert(table: str, source_schema: str, dest_schema: str, key: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    log.info(f"Starting full sync (DELETE+INSERT) for table {table}.")
    
    delete_sql, insert_sql = generate_delete_insert_sqls(hook, source_schema, dest_schema, table, key)
    
    try:
        log.info(f"Executing DELETE for full table {table}.")
        hook.run(delete_sql)
        log.info(f"Executing INSERT for full table {table}.")
        hook.run(insert_sql)
        log.info(f"Full sync (DELETE+INSERT) completed for table {table}.")
    except Exception as e:
        log.error(f"Failed to sync full table {table} using DELETE+INSERT: {e}")
        raise

with DAG(
    dag_id="trino_delete_insert_all_tables_partitioned", 
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=10, 
    tags=["trino", "data-sync", "delete-insert"]
) as dag:

    partitioned_tables = {
        "diachi": ("matinh", NDC_VUNGTAPKET_BCA, "madddiadiem"),
        "giaytodinhdanhcn": ("sogiayto", NDC_VUNGTAPKET_BCA, "sogiayto"),
        "nguoivn": ("sodinhdanh", NDC_VUNGTAPKET_BCA, "sodinhdanh"),
    }

    for table, (partition_field, source_schema, key) in partitioned_tables.items():
        create_task = create_table_if_not_exists.override(task_id=f"create_table_{table}")(
            table_name=table, source_schema=source_schema, dest_schema=DEST_SCHEMA_DAN_CU
        )

        with TaskGroup(group_id=f"{table}_sync_partitions") as tg_partitions:
            for digit in get_partitions_last_digit():
                sync_task = sync_partition_delete_insert.override(task_id=f"sync_{table}_partition_{digit}")(
                    table=table,
                    source_schema=source_schema,
                    dest_schema=DEST_SCHEMA_DAN_CU,
                    partition_field=partition_field,
                    last_digit=digit,
                    key=key
                )
        create_task >> tg_partitions

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
        create_task = create_table_if_not_exists.override(task_id=f"create_table_{table}")(
            table_name=table, source_schema=source_schema, dest_schema=DEST_SCHEMA_DANH_MUC
        )
        sync_task = sync_full_table_delete_insert.override(task_id=f"sync_{table}")(
            table=table,
            source_schema=source_schema,
            dest_schema=DEST_SCHEMA_DANH_MUC,
            key=key
        )
        create_task >> sync_task
