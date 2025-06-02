from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.decorators import task
from datetime import datetime, timedelta
from typing import List, Set

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc"

NDC_VUNGTAPKET_BCA = "ndc-khotapket-bca"
NDA_VUNGTAPKET_DANHMUC = "ndc-khotapket-danhmuc"

DEST_SCHEMA = "ndc-khodieuphoi-dancu-pa"
VUNGDUNGCHUNG_DANHMUC = "ndc-khodieuphoi-danhmuc-pa"

default_args = {
    "owner": "ND",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

def get_columns(hook: TrinoHook, schema: str, table: str) -> List[str]:
    sql = f"""
    SELECT column_name
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    results = hook.get_records(sql)
    return [row[0] for row in results if not row[0].startswith("_")]

def generate_merge_sql(
    hook: TrinoHook,
    source_schema: str,
    dest_schema: str,
    table: str,
    key: str,
    condition: str = None
) -> str:
    columns = get_columns(hook, source_schema, table)

    update_columns = [col for col in columns if col != key]

    escaped_columns = [f'"{col}"' for col in columns]
    update_set = ", ".join([f'"{col}" = source."{col}"' for col in update_columns])
    insert_values = ", ".join([f'source."{col}"' for col in columns])
    columns_str = ", ".join([f'"{col}"' for col in columns])
    where_clause = f"WHERE {condition}" if condition else ""

    return f"""
    MERGE INTO {CATALOG}."{dest_schema}"."{table}" AS target
    USING (
        SELECT {columns_str}
        FROM {CATALOG}."{source_schema}"."{table}"
        {where_clause}
    ) AS source
    ON target."{key}" = source."{key}"
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT ({columns_str}) VALUES ({insert_values})
    """

@task
def create_schema_if_not_exists(schema_name: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f'CREATE SCHEMA IF NOT EXISTS {CATALOG}."{schema_name}"'
    hook.run(sql)

@task
def create_table_if_not_exists(
    table_name: str,
    source_schema: str,
    dest_schema: str,
    partitioning: List[str] = None 
):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    columns = get_columns(hook, source_schema, table_name)
    if not columns:
        raise ValueError(f"Không tìm thấy cột nào hợp lệ trong bảng {source_schema}.{table_name}")

    column_list = ", ".join([f'"{col}"' for col in columns])
    partitioning_clause = ""

    if partitioning:
        # Convert list to Trino SQL ARRAY[...] syntax
        partitioning_sql = ",\n        ".join([f"'{p}'" for p in partitioning])
        partitioning_clause = f""",
    partitioning = ARRAY[
        {partitioning_sql}
    ]"""

    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}."{dest_schema}"."{table_name}" 
    WITH (
        format = 'PARQUET'{partitioning_clause}
    ) AS
    SELECT {column_list}
    FROM {CATALOG}."{source_schema}"."{table_name}"
    WHERE 1=0
    """
    hook.run(sql)

@task
def merge_full_table(table: str, source_schema: str, dest_schema: str, key: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = generate_merge_sql(hook, source_schema, dest_schema, table, key)
    hook.run(sql)

with DAG(
    dag_id="dong_bo_du_lieu_tu_tap_ket_sang_dieu_phoi",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    tags=["trino", "data-merge"]
) as dag:

    all_tables = [
        ("diachi", NDC_VUNGTAPKET_BCA, DEST_SCHEMA, "madddiadiem", ["bucket(madddiadiem, 100)"]),
        ("giaytodinhdanhcn", NDC_VUNGTAPKET_BCA, DEST_SCHEMA, "sogiayto", ["bucket(sogiayto, 100)", "loaigiayto"]),
        ("nguoivn", NDC_VUNGTAPKET_BCA, DEST_SCHEMA, "sodinhdanh", ["bucket(sodinhdanh, 10)", "trangthai", "gioitinh"]),
        ("dm_dantoc", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_giatrithithuc", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_gioitinh", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_huyen", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_loaigiaytotuythan", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_loaigiaytoxnc", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_nhommau", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_quoctich", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "maquocgia", []),
        ("dm_tinh", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_tongiao", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
        ("dm_xa", NDA_VUNGTAPKET_DANHMUC, VUNGDUNGCHUNG_DANHMUC, "ma", []),
    ]

    dest_schemas: Set[str] = set([dest for _, _, dest, _, _ in all_tables])
    schema_tasks = {}

    for schema in dest_schemas:
        schema_tasks[schema] = create_schema_if_not_exists.override(task_id=f"tao_schema_{schema}")(schema)

    for table, source_schema, dest_schema, key, partitioning in all_tables:
        create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table_name=table,source_schema= source_schema,dest_schema= dest_schema, partitioning=partitioning)
        merge_table = merge_full_table.override(task_id=f"dong_bo_{table}")(table, source_schema, dest_schema, key)

        schema_tasks[dest_schema] >> create_table >> merge_table
