from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

# Push dữ liệu vào XCom
def push_to_xcom(**kwargs):
    value = "Hello from push_task 👋"
    kwargs['ti'].xcom_push(key='message', value=value)
    logging.info("✅ Đã push message vào XCom")

# Pull dữ liệu từ XCom
def pull_from_xcom(**kwargs):
    ti = kwargs['ti']
    message = ti.xcom_pull(task_ids='push_task', key='message')
    logging.info(f"📥 Đã pull từ XCom: {message}")

# Task kết thúc
def end_task_func():
    logging.info("🏁 Kết thúc DAG.")

# Định nghĩa DAG
with DAG(
    dag_id='Phong_DAG_XCom',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['xcom', 'phong'],
    description='DAG demo XCom push & pull',
) as dag:

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_to_xcom,
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_from_xcom,
    )

    end_task = PythonOperator(
        task_id='end_task',
        python_callable=end_task_func,
    )

    # Thiết lập thứ tự chạy
    push_task >> pull_task >> end_task
