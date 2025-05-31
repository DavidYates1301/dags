from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import List
import logging

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc"
NDC_VUNGTAPKET_BCA = "ndc_vungtapket_bca"
NDA_VUNGTAPKET_DANHMUC = "ndc_vungtapket_danhmuc"
DEST_SCHEMA_DAN_CU = "ndc_vungdungchung_dancu"
DEST_SCHEMA_DANH_MUC = "ndc_vungdungchung_danhmuc"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2, 
    "retry_delay": timedelta(seconds=10),
    "email_on_failure": False, 
    "email_on_retry": False,
}

def get_partitions_last_digit() -> List[str]:
    """Trả về danh sách các chữ số cuối cùng (0-9) để phân chia."""
    return [str(i) for i in range(10)]

def get_columns(logger: logging.Logger, hook: TrinoHook, schema: str, table: str) -> List[str]:
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
    logger.info(f"Fetching columns for {CATALOG}.{schema}.{table}")
    results = hook.get_records(sql)
    columns = [row[0] for row in results]
    if not columns:
        logger.warning(f"No columns found for table {CATALOG}.{schema}.{table}. This might indicate a table does not exist or has no defined columns.")
    logger.info(f"Columns for {schema}.{table}: {columns}")
    return columns

def generate_delete_insert_sqls(logger: logging.Logger, hook: TrinoHook, source_schema: str, dest_schema: str, table: str, key: str, condition: str = None) -> (str, str):
    """
    Tạo các câu lệnh SQL DELETE và INSERT để đồng bộ hóa dữ liệu.
    """
    columns = get_columns(logger, hook, source_schema, table)
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

    # Câu lệnh INSERT: Thêm tất cả các bản ghi từ bảng nguồn vào bảng đích
    # (chỉ trong phân vùng nếu có điều kiện)
    insert_sql = f"""
    INSERT INTO {CATALOG}.{dest_schema}.{table} ({columns_str})
    SELECT {columns_str}
    FROM {CATALOG}.{source_schema}.{table} AS source
    {where_clause}
    """
    
    logger.info(f"Generated DELETE SQL for {table} (partition: {condition if condition else 'full'}): \n{delete_sql}")
    logger.info(f"Generated INSERT SQL for {table} (partition: {condition if condition else 'full'}): \n{insert_sql}")
    
    return delete_sql, insert_sql

@task
def create_table_if_not_exists(table_name: str, source_schema: str, dest_schema: str):
    """
    Tạo bảng đích nếu nó chưa tồn tại, với schema tương tự như bảng nguồn.
    Sử dụng WHERE 1=0 để chỉ sao chép cấu trúc mà không sao chép dữ liệu.
    """
    task_log = logging.getLogger(__name__)
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{dest_schema}.{table_name} AS
    SELECT * FROM {CATALOG}.{source_schema}.{table_name} WHERE 1=0
    """
    task_log.info(f"Attempting to create table {CATALOG}.{dest_schema}.{table_name} if not exists.")
    try:
        hook.run(sql)
        task_log.info(f"Table {CATALOG}.{dest_schema}.{table_name} creation check completed successfully.")
        return {"create_sql": sql}
    except Exception as e:
        task_log.error(f"Failed to create table {CATALOG}.{dest_schema}.{table_name}: {e}")
        raise 

@task
def sync_partition_delete_insert(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str, key: str):
    """
    Thực hiện đồng bộ hóa cho một phân vùng cụ thể của bảng bằng cách DELETE và sau đó INSERT.
    """
    task_log = logging.getLogger(__name__)
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    condition = f"substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'"
    task_log.info(f"Starting sync (DELETE+INSERT) for partitioned table {table}, partition {last_digit}.")
    
    delete_sql, insert_sql = generate_delete_insert_sqls(task_log, hook, source_schema, dest_schema, table, key, condition)
    
    try:
        task_log.info(f"Executing DELETE for {table}, partition {last_digit}.")
        hook.run(delete_sql)
        task_log.info(f"Executing INSERT for {table}, partition {last_digit}.")
        hook.run(insert_sql)
        task_log.info(f"Sync (DELETE+INSERT) completed for partitioned table {table}, partition {last_digit}.")
        return {"delete_sql": delete_sql, "insert_sql": insert_sql}
    except Exception as e:
        task_log.error(f"Failed to sync partition {last_digit} for table {table} using DELETE+INSERT: {e}")
        raise

@task
def sync_full_table_delete_insert(table: str, source_schema: str, dest_schema: str, key: str):
    """
    Thực hiện đồng bộ hóa cho toàn bộ bảng không phân vùng bằng cách DELETE và sau đó INSERT.
    """
    task_log = logging.getLogger(__name__)
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    task_log.info(f"Starting full sync (DELETE+INSERT) for table {table}.")
    
    delete_sql, insert_sql = generate_delete_insert_sqls(task_log, hook, source_schema, dest_schema, table, key)
    
    try:
        task_log.info(f"Executing DELETE for full table {table}.")
        hook.run(delete_sql)
        task_log.info(f"Executing INSERT for full table {table}.")
        hook.run(insert_sql)
        task_log.info(f"Full sync (DELETE+INSERT) completed for table {table}.")
        return {"delete_sql": delete_sql, "insert_sql": insert_sql}
    except Exception as e:
        task_log.error(f"Failed to sync full table {table} using DELETE+INSERT: {e}")
        raise

with DAG(
    dag_id="trino_delete_insert_all_tables_partitioned", 
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5, 
    tags=["trino", "data-sync", "delete-insert", "debug"] 
) as dag:

    # ======== Cấu hình BẢNG PHÂN MẢNH (Partitioned Tables) ========
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
