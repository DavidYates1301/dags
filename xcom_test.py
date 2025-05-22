from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

def _generate_and_push_xcom_auto(**kwargs):
    """
    Tạo một dictionary và trả về nó.
    Airflow sẽ tự động đẩy giá trị trả về này lên XCom.
    """
    task_id = kwargs["task_instance"].task_id
    current_time = pendulum.now().isoformat()
    data = {
        "message": f"Hello from {task_id}!",
        "generated_at": current_time,
        "source": "auto_xcom_push",
        "random_number": kwargs["ti"].dag_run.run_id 
    }
    print(f"[{task_id}] Pushing data automatically: {data}")
    return data

def _receive_xcom_auto(**kwargs):
    """
    Nhận dữ liệu từ XCom của task trước.
    Airflow tự động pull giá trị từ XCom của task trước theo tên task.
    """
    ti = kwargs["ti"]
    received_data = ti.xcom_pull(task_ids="generate_xcom_auto_task")
    print(f"[receive_xcom_auto] Received data automatically: {received_data}")

    if received_data and "message" in received_data:
        print(f"[{ti.task_id}] Success! Received message: {received_data['message']}")
        return "XCom Auto Test Succeeded"
    else:
        raise ValueError("Failed to receive XCom data automatically!")

def _generate_and_push_xcom_manual(**kwargs):
    """
    Tạo một dictionary và đẩy nó lên XCom bằng xcom_push.
    """
    ti = kwargs["ti"]
    task_id = ti.task_id
    current_time = pendulum.now().isoformat()
    data = {
        "greeting": f"Greetings from {task_id}!",
        "pushed_at": current_time,
        "method": "manual_xcom_push",
        "some_id": ti.dag_run.run_id
    }
    print(f"[{task_id}] Pushing data manually: {data}")
    ti.xcom_push(key="my_custom_key", value=data)

def _receive_xcom_manual(**kwargs):
    """
    Nhận dữ liệu từ XCom của task trước bằng xcom_pull với key cụ thể.
    """
    ti = kwargs["ti"]
    received_data = ti.xcom_pull(task_ids="generate_xcom_manual_task", key="my_custom_key")
    print(f"[receive_xcom_manual] Received data manually: {received_data}")

    if received_data and "greeting" in received_data:
        print(f"[{ti.task_id}] Success! Received greeting: {received_data['greeting']}")
        return "XCom Manual Test Succeeded"
    else:
        raise ValueError("Failed to receive XCom data manually!")

with DAG(
    dag_id="xcom_kubernetes_executor_test",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["xcom", "kubernetes", "test"],
    doc_md="""
    ### XCom KubernetesExecutor Test DAG
    This DAG demonstrates and tests XCom functionality when running on KubernetesExecutor
    with multiple schedulers, triggers, and dag-processors.

    It uses two methods:
    1. Automatic XCom push/pull (return value of PythonOperator).
    2. Manual XCom push/pull using `xcom_push` and `xcom_pull` with a custom key.
    """
) as dag:
    generate_xcom_auto_task = PythonOperator(
        task_id="generate_xcom_auto_task",
        python_callable=_generate_and_push_xcom_auto,
    )

    receive_xcom_auto_task = PythonOperator(
        task_id="receive_xcom_auto_task",
        python_callable=_receive_xcom_auto,
    )

    generate_xcom_manual_task = PythonOperator(
        task_id="generate_xcom_manual_task",
        python_callable=_generate_and_push_xcom_manual,
    )

    receive_xcom_manual_task = PythonOperator(
        task_id="receive_xcom_manual_task",
        python_callable=_receive_xcom_manual,
    )

for upstream_task in [generate_xcom_auto_task, generate_xcom_manual_task]:
    for downstream_task in [receive_xcom_auto_task, receive_xcom_manual_task]:
        upstream_task >> downstream_task
