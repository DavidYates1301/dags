from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# Task đẩy dữ liệu lên XCom
def _push_xcom_data(**kwargs):
    """
    Tạo một dictionary đơn giản và đẩy nó lên XCom.
    """
    data_to_push = {"my_key": "Hello from XCom!", "value": 12345}
    print(f"[{kwargs['task_instance'].task_id}] Pushing data: {data_to_push}")
    kwargs["ti"].xcom_push(key="my_simple_xcom_key", value=data_to_push)

# Task kéo dữ liệu từ XCom
def _pull_xcom_data(**kwargs):
    """
    Kéo dữ liệu từ XCom của task trước.
    """
    ti = kwargs["ti"]
    # Kéo giá trị XCom với key 'my_simple_xcom_key' từ task 'push_data_task'
    received_data = ti.xcom_pull(task_ids="push_data_task", key="my_simple_xcom_key")

    print(f"[{ti.task_id}] Received data: {received_data}")

    if received_data and received_data.get("my_key") == "Hello from XCom!":
        print(f"[{ti.task_id}] XCom test successful! Data matches.")
    else:
        raise ValueError("XCom test failed: Data not received or not matching.")


with DAG(
    dag_id="xcom_simple_kubernetes_test", # ID DAG mới, đơn giản hơn
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["xcom", "kubernetes", "simple"],
    doc_md="""
    ### XCom Simple Kubernetes Test DAG
    This DAG provides a basic test for XCom functionality
    on KubernetesExecutor.
    It pushes a simple value and then pulls it.
    """
) as dag:
    # Task đẩy dữ liệu
    push_data_task = PythonOperator(
        task_id="push_data_task",
        python_callable=_push_xcom_data,
    )

    # Task kéo dữ liệu
    pull_data_task = PythonOperator(
        task_id="pull_data_task",
        python_callable=_pull_xcom_data,
    )

    # Định nghĩa luồng công việc đơn giản
    push_data_task >> pull_data_task
