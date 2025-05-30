from airflow import DAG
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import math

SOURCE_CONN_ID = "trino_default"
CATALOG = "ndc_common_zone_masterdata"
SOURCE_SCHEMA = "masterdata"
DEST_SCHEMA = "destination"
TABLE_NAME = "giay_to_dinh_danh_cn"
PARTITION_FIELD = "loaigiayto"
PARTITION_VALUES = [str(i) for i in range(1, 9)]
BATCH_SIZE = 5000

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=2),
}

def create_destination_table():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME} AS
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME} WHERE 1=0
    """
    hook.run(sql)
    print(f"Table {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME} created or already exists.")

def get_partition_counts():
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    SELECT {PARTITION_FIELD}, COUNT(*) as cnt
    FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    GROUP BY {PARTITION_FIELD}
    """
    records = hook.get_records(sql)
    counts = {row[0]: row[1] for row in records}
    print("Partition counts:", counts)
    return counts

def copy_partition_batch(loaigiayto_value: str, offset: int):
    hook = TrinoHook(trino_conn_id=SOURCE_CONN_ID)
    sql = f"""
    INSERT INTO {CATALOG}.{DEST_SCHEMA}.{TABLE_NAME}
    SELECT * FROM {CATALOG}.{SOURCE_SCHEMA}.{TABLE_NAME}
    WHERE {PARTITION_FIELD} = '{loaigiayto_value}'
    ORDER BY id  -- hoặc cột có thể sắp xếp ổn định
    OFFSET {offset} LIMIT {BATCH_SIZE}
    """
    hook.run(sql)
    print(f"Copied loaigiayto={loaigiayto_value}, offset={offset}, batch_size={BATCH_SIZE}")

with DAG(
    dag_id="trino_copy_partitioned_batches_real_counts",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=5,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    create_table = PythonOperator(
        task_id="create_destination_table",
        python_callable=create_destination_table,
    )

    get_counts = PythonOperator(
        task_id="get_partition_counts",
        python_callable=get_partition_counts,
    )

    start >> create_table >> get_counts

    def generate_batch_tasks(**context):
        from airflow.models import DagRun
        from airflow.operators.python import task

        ti = context["ti"]
        counts = ti.xcom_pull(task_ids="get_partition_counts")

        for loaigiayto_value, total in counts.items():
            num_batches = math.ceil(total / BATCH_SIZE)
            for i in range(num_batches):
                offset = i * BATCH_SIZE
                task_id = f"copy_loaigiayto_{loaigiayto_value}_batch_{i}"

                PythonOperator(
                    task_id=task_id,
                    python_callable=copy_partition_batch,
                    op_args=[loaigiayto_value, offset],
                    dag=dag,
                ).set_upstream(get_counts) >> end

    gen_task = PythonOperator(
        task_id="generate_batch_tasks",
        python_callable=generate_batch_tasks,
    )

    get_counts >> gen_task
