from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='example_dag_with_pools',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='A DAG with 5 tasks using a pool with 2 slots',
) as dag:

    task_1 = BashOperator(
        task_id='task_1',
        bash_command='sleep 5',
        pool='my_pool'
    )

    task_2 = BashOperator(
        task_id='task_2',
        bash_command='sleep 5',
        pool='my_pool'
    )

    task_3 = BashOperator(
        task_id='task_3',
        bash_command='sleep 5',
        pool='my_pool'
    )

    task_4 = BashOperator(
        task_id='task_4',
        bash_command='sleep 5',
        pool='my_pool'
    )

    task_5 = BashOperator(
        task_id='task_5',
        bash_command='sleep 5',
        pool='my_pool'
    )

    # Set dependencies (you can adjust this as needed)
    [task_1, task_2, task_3, task_4] >> task_5
