from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# Định nghĩa DAG
with DAG(
    dag_id='Phong_DAG',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',    
    catchup=False,        
    tags=['example', 'phong'],  
    description='A simple DAG with 3 tasks using Airflow 3.0.1',
) as dag:

    # Task bắt đầu
    start_task = EmptyOperator(
        task_id='start'
    )

    # Task ở giữa
    middle_task = EmptyOperator(
        task_id='middle'
    )

    # Task kết thúc
    end_task = EmptyOperator(
        task_id='end'
    )

    # Thiết lập thứ tự thực thi
    start_task >> middle_task >> end_task
