from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_xcom_value(ti):
    message = "Hello from task 1!"
    ti.xcom_push(key='my_key', value=message)

def pull_xcom_value(ti):
    pulled_message = ti.xcom_pull(key='my_key', task_ids='push_task')
    print(f"Received message: {pulled_message}")

with DAG(
    dag_id='xcom_example_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Sửa chỗ này nếu dùng Airflow >= 3.0
    catchup=False,
    tags=['example', 'xcom']
) as dag:

    push_task = PythonOperator(
        task_id='push_task',
        python_callable=push_xcom_value
    )

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_xcom_value
    )

    push_task >> pull_task
