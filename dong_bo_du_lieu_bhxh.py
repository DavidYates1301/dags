from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.decorators import task
from airflow.models import Variable 
from datetime import datetime, timedelta
from typing import List, Set
from airflow.utils.task_group import TaskGroup


SOURCE_CONN_ID = "trino_default"
CATALOG = Variable.get("catalog")

NDC_KHO_TAP_KET_DANCU = Variable.get("source_schema_bca", default_var="ndc-khotapket-bca")
NDC_KHO_TAP_KET_DANHMUC = Variable.get("source_schema_danhmuc", default_var="ndc-khotapket-danhmuc")
NDC_KHOTAPKET_BHXH = Variable.get("source_schema_bhxh", default_var="ndc-khotapket-bhxh")


KHODIEUPHOI_DANCU = Variable.get("dest_schema_dancu", default_var="ndc-khodieuphoi-dancu")
KHODIEUPHOI_DANHMUC = Variable.get("dest_schema_danhmuc", default_var="ndc-khodieuphoi-danhmuc")
KHODIEUPHOIBHXH =  Variable.get("dest_schema_bhxh", default_var="ndc-khodieuphoi-bhxh")


default_args = {
    "owner": "ND",
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
def create_table_if_not_exists(table_name: str, source_schema: str, dest_schema: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}."{dest_schema}"."{table_name}" AS
    SELECT * FROM {CATALOG}."{source_schema}"."{table_name}" WHERE 1=0
    """
    hook.run(sql)

@task
def merge_full_table(table: str, source_schema: str, dest_schema: str, key: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = generate_merge_sql(hook, source_schema, dest_schema, table, key)
    hook.run(sql)

@task
def merge_partition(table: str, source_schema: str, dest_schema: str, partition_field: str, last_digit: str, key: str):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    condition = f"substr(trim(cast({partition_field} as varchar)), -1) = '{last_digit}'"
    sql = generate_merge_sql(hook, source_schema, dest_schema, table, key, condition)
    hook.run(sql)


with DAG(
    dag_id="dong_bo_du_lieu_bhxh_tu_tap_ket_sang_dieu_phoi", 
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=4,
    tags=["trino", "data-merge", "variables"]
) as dag:
    
    trigger_sync_task1 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_du_lieu_ndel_danh_muc_bhxh',
        airbyte_conn_id='ndel_default',              
        connection_id='ac3ad9f2-218c-469a-8f89-ec236515ac5c', 
        asynchronous=False                                    
    )

    trigger_sync_task2 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_du_lieu_ndel_tham_gia_bh',
        airbyte_conn_id='ndel_default',              
        connection_id='6a36040f-6b5f-44b9-a643-4c48e71a9563', 
        asynchronous=False                                    
    )

    trigger_sync_task3 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_du_lieu_ndel_chi_tra_bh',
        airbyte_conn_id='ndel_default',              
        connection_id='2b87234e-3ecf-499e-aeca-5230d539a164', 
        asynchronous=False                                    
    )

    trigger_sync_task4 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_du_lieu_ndel_huong_bh',
        airbyte_conn_id='ndel_default',              
        connection_id='24e93629-0083-4b25-ad89-68f3705c8096', 
        asynchronous=False                                    
    )

    trigger_sync_task5 = AirbyteTriggerSyncOperator(
        task_id='dong_bo_du_lieu_ndel_nguoi_tham_gia_bh',
        airbyte_conn_id='ndel_default',              
        connection_id='68a7259b-5d21-4aaf-a022-894a71d60906', 
        asynchronous=False                                    
    )




    dm_tables = [
        ("dm_chedohuong", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_chuongbenh", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_loai_benh", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "mabenh"),
        ("dm_loaidoituongbhyt", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_cosokhamchuabenh", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_hinhthucthamgia", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_khoithongke", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_mabenh", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_maloaibenh", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_mamuchuongbhyt", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC,"ma"),
        ("dm_nhombenh", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma"),
        ("dm_phuongthucdong", NDC_KHO_TAP_KET_DANHMUC, KHODIEUPHOI_DANHMUC, "ma")
    ]

    tham_gia_bao_hiem_tables = [
        ("thamgiabhtn", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "sodinhdanh"),
        ("thamgiabhxh", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "sodinhdanh"),
        ("thamgiabhyt", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "mathe"),
    ]

    chi_tra_tables = [
        ("chitra", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "ma"),
        ("quatrinhchitra", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "ma"),
    ]

    huong_bao_hiem_tables = [
        ("huongbhxh", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "ma"),
        ("huongbhtn", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "ma"),
        ("huongbhyt", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "ma"),
        ("quatrinhhuongbhxh", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "sodinhdanh"),
        ("quatrinhhuongbhtn", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "sodinhdanh"),
        ("quatrinhhuongbhyt", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "sodinhdanh"),
        ("quyetdinhhuongbhtn", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "sodinhdanh"),
    ]

    nguoi_tham_gia_bh_tables = [
        ("nguoithamgiabaohiem", NDC_KHOTAPKET_BHXH, KHODIEUPHOIBHXH, "sodinhdanh")
    ]

    
    create_schema_danhmuc = create_schema_if_not_exists.override(task_id="tao_schema_khodieuphoi_danhmuc")(schema_name=KHODIEUPHOI_DANHMUC)
    create_schema_bhxh = create_schema_if_not_exists.override(task_id="tao_schema_khodieuphoi_bhxh")(schema_name=KHODIEUPHOIBHXH)
    
    
    trigger_sync_task1 >> [trigger_sync_task2, create_schema_danhmuc]
    trigger_sync_task2 >> [trigger_sync_task3, create_schema_bhxh] >> trigger_sync_task4 >> trigger_sync_task5




    for table, source_schema, dest_schema, key in tham_gia_bao_hiem_tables:
        create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
        [trigger_sync_task2, create_schema_bhxh] >> create_table
        with TaskGroup(group_id=f"dong_bo_du_lieu_{table}") as tg:
            for digit in get_partitions_last_digit():
                merge_task = merge_partition.override(task_id=f"dong_bo_{table}_{digit}")(
                    table=table, source_schema=source_schema, dest_schema=dest_schema, partition_field=key, last_digit=digit, key=key
                )
                create_table >> merge_task
        create_table >> tg
        


    for table, source_schema, dest_schema, key in chi_tra_tables:
        create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
        [trigger_sync_task3, create_schema_bhxh] >> create_table
        with TaskGroup(group_id=f"dong_bo_du_lieu_{table}") as tg:
            for digit in get_partitions_last_digit():
                merge_task = merge_partition.override(task_id=f"dong_bo_{table}_{digit}")(
                    table=table, source_schema=source_schema, dest_schema=dest_schema, partition_field=key, last_digit=digit, key=key
                )
                create_table >> merge_task
        create_table >> tg


    for table, source_schema, dest_schema, key in huong_bao_hiem_tables:
        create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
        [trigger_sync_task4, create_schema_bhxh] >> create_table
        with TaskGroup(group_id=f"dong_bo_du_lieu_{table}") as tg:
            for digit in get_partitions_last_digit():
                merge_task = merge_partition.override(task_id=f"dong_bo_{table}_{digit}")(
                    table=table, source_schema=source_schema, dest_schema=dest_schema, partition_field=key, last_digit=digit, key=key
                )
                create_table >> merge_task
        create_table >> tg

    for table, source_schema, dest_schema, key in nguoi_tham_gia_bh_tables:
        create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
        [trigger_sync_task5, create_schema_bhxh] >> create_table
        with TaskGroup(group_id=f"dong_bo_du_lieu_{table}") as tg:
            for digit in get_partitions_last_digit():
                merge_task = merge_partition.override(task_id=f"dong_bo_{table}_{digit}")(
                    table=table, source_schema=source_schema, dest_schema=dest_schema, partition_field=key, last_digit=digit, key=key
                )
                create_table >> merge_task
        create_table >> tg
        
    

    
    for table, source_schema, dest_schema, key in dm_tables:
        create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
        merge_table = merge_full_table.override(task_id=f"dong_bo_{table}")(table, source_schema, dest_schema, key)

        [trigger_sync_task2, create_schema_bhxh] >> create_table >> merge_table

    # for table, source_schema, dest_schema, key in chi_tra_tables:
    #     create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
    #     merge_table = merge_full_table.override(task_id=f"dong_bo_{table}")(table, source_schema, dest_schema, key)

    #     [trigger_sync_task3, create_schema_bhxh] >> create_table >> merge_table

    # for table, source_schema, dest_schema, key in huong_bao_hiem_tables:
    #     create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
    #     merge_table = merge_full_table.override(task_id=f"dong_bo_{table}")(table, source_schema, dest_schema, key)

    #     [trigger_sync_task4, create_schema_bhxh] >> create_table >> merge_table

    # for table, source_schema, dest_schema, key in nguoi_tham_gia_bh_tables:
    #     create_table = create_table_if_not_exists.override(task_id=f"tao_bang_{table}")(table, source_schema, dest_schema)
    #     merge_table = merge_full_table.override(task_id=f"dong_bo_{table}")(table, source_schema, dest_schema, key)

    #     [trigger_sync_task5, create_schema_bhxh] >> create_table >> merge_table




    
