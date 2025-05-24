from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_success(task_number):
    print(f"Task {task_number} executed successfully.")

def task_with_error():
    raise ValueError("This is an intentional error in task_3.")

with DAG(
    dag_id='Phong_DAG_Error',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example"]
) as dag:

    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=lambda: task_success(1)
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=lambda: task_success(2)
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=task_with_error
    )

    task_4 = PythonOperator(
        task_id='task_4',
        python_callable=lambda: task_success(4)
    )

    task_5 = PythonOperator(
        task_id='task_5',
        python_callable=lambda: task_success(5)
    )

    # Định nghĩa luồng thực thi
    task_1 >> task_2 >> task_3 >> task_4 >> task_5
