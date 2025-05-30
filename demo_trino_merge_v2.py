from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import List
import logging

log = logging.getLogger(__name__)

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc"
NDC_VUNGTAPKET_BCA = "ndc_vungtapket_bca"
NDA_VUNGTAPKET_DANHMUC = "ndc_vungtapket_danhmuc"
# Đổi tên biến schema đích để rõ ràng hơn
DEST_SCHEMA_DAN_CU = "ndc_vungdungchung_dancu"
DEST_SCHEMA_DANH_MUC = "ndc_vungdungchung_danhmuc"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=2), 
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

def generate_merge_sql(hook: TrinoHook, source_schema: str, dest_schema: str, table: str, key: str, condition: str = None) -> str:
    """
    Tạo câu lệnh SQL MERGE để đồng bộ hóa dữ liệu giữa bảng nguồn và bảng đích.
    Sử dụng upsert (INSERT nếu không khớp, UPDATE nếu khớp).
    """
    columns = get_columns(hook, source_schema, table)
    
    escaped_columns = [f'"{col}"' for col in columns]
    
    columns_str = ", ".join(escaped_columns)
    
    insert_values_str = ", ".join([f"source.{col}" for col in escaped_columns])

    updateable_columns = [col for col in escaped_columns if col.strip('"') != key]
    update_set_clause = ", ".join([f'target.{col} = source.{col}' for col in updateable_columns])

    where_clause = f"WHERE {condition}" if condition else ""

    merge_sql_parts = [f"""
    MERGE INTO {CATALOG}.{dest_schema}.{table} AS target
    USING (
        SELECT {columns_str} FROM {CATALOG}.{source_schema}.{table} {where_clause}
    ) AS source
    ON target."{key}" = source."{key}"
    """]

    # Thêm mệnh đề WHEN MATCHED chỉ khi có các cột để cập nhật
    if update_set_clause:
        merge_sql_parts.append(f"""
    WHEN MATCHED THEN UPDATE SET {update_set_clause}
        """)
    else:
        log.warning(f"Table '{table}' has no updateable columns besides the key '{key}'. "
                    f"The MERGE statement will only include 'WHEN NOT MATCHED THEN INSERT'.")

    # Luôn thêm mệnh đề WHEN NOT MATCHED
    merge_sql_parts.append(f"""
    WHEN NOT MATCHED THEN INSERT ({columns_str}) VALUES ({insert_values_str})
    """)
    
    final_merge_sql = "".join(merge_sql_parts)
    log.info(f"Generated MERGE SQL for {table} (partition: {condition if condition else 'full'}): \n{final_merge_sql}")
    return final_merge_sql

@task
def create_table_if_not_exists(table_name: str, source_schema: str, dest_schema: str):
    """
    Tạo bảng đích nếu nó chưa tồn tại, với schema tương tự như bảng nguồn.
    Sử dụng WHERE 1=0 để chỉ sao chép cấu trúc mà không sao chép dữ liệu.
    """
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
def merge_partition(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str, key: str):
    """
    Thực hiện MERGE cho một phân vùng cụ thể của bảng.
    """
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    condition = f"substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'"
    log.info(f"Starting merge for partitioned table {table}, partition {last_digit}.")
    sql = generate_merge_sql(hook, source_schema, dest_schema, table, key, condition)
    try:
        hook.run(sql)
        log.info(f"Merge completed for partitioned table {table}, partition {last_digit}.")
    except Exception as e:
        log.error(f"Failed to merge partition {last_digit} for table {table}: {e}")
        raise

@task
def merge_full_table(table: str, source_schema: str, dest_schema: str, key: str):
    """
    Thực hiện MERGE cho toàn bộ bảng không phân vùng.
    """
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    log.info(f"Starting full merge for table {table}.")
    sql = generate_merge_sql(hook, source_schema, dest_schema, table, key)
    try:
        hook.run(sql)
        log.info(f"Full merge completed for table {table}.")
    except Exception as e:
        log.error(f"Failed to merge full table {table}: {e}")
        raise

with DAG(
    dag_id="trino_merge_all_tables_partitioned_v2", 
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=10, 
    tags=["trino", "data-merge", "fix"]
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
                merge_task = merge_partition.override(task_id=f"merge_{table}_partition_{digit}")(
                    table=table,
                    source_schema=source_schema,
                    dest_schema=DEST_SCHEMA_DAN_CU,
                    partition_field=partition_field,
                    last_digit=digit,
                    key=key
                )
        create_task >> tg_partitions

    # ======== Cấu hình BẢNG KHÔNG PHÂN MẢNH (Non-Partitioned Tables) ======== #
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
        merge_task = merge_full_table.override(task_id=f"merge_{table}")(
            table=table,
            source_schema=source_schema,
            dest_schema=DEST_SCHEMA_DANH_MUC,
            key=key
        )
        create_task >> merge_task
