from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

# --- Config ---
DBT_REPO_URL = "https://github.com/DavidYates1301/dbt_project.git"
DBT_REPO_BRANCH = "main"
DBT_PROJECT_PATH = "/opt/airflow/dbt/project"
DBT_PROFILES_PATH = "/opt/airflow/dbt_profiles"
DBT_PROFILE_NAME = "dbt_scheduler"
DBT_IMAGE = "192.168.1.67:9082/dbt-runner:1.7.0-v3"
K8S_NAMESPACE = "cd-scheduler"
DBT_IMAGE_PULL_SECRET = "nexus-pull-secret"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_postgres_kubernetes_example",
    start_date=datetime(2023, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "kubernetes", "postgres"],
    description="Run dbt models on Kubernetes",
) as dag:

    # Shared Volumes
    dbt_project_volume = k8s.V1Volume(name="dbt-project-volume", empty_dir=k8s.V1EmptyDirVolumeSource())
    dbt_project_mount = k8s.V1VolumeMount(name="dbt-project-volume", mount_path="/opt/airflow/dbt")

    dbt_profiles_volume = k8s.V1Volume(
        name="dbt-profiles-volume",
        secret=k8s.V1SecretVolumeSource(secret_name="dbt-profiles")
    )
    dbt_profiles_mount = k8s.V1VolumeMount(
        name="dbt-profiles-volume",
        mount_path=DBT_PROFILES_PATH,
        read_only=True
    )

    # Task 1: Clone dbt repo using git-sync
    git_sync_dbt_project = KubernetesPodOperator(
        task_id="git_sync_dbt_project",
        namespace=K8S_NAMESPACE,
        image="registry.k8s.io/git-sync/git-sync:v4.4.0",
        arguments=[
            f"--repo={DBT_REPO_URL}",
            f"--ref={DBT_REPO_BRANCH}",
            "--root=/opt/airflow/dbt",
            "--link=project",
            "--one-time",
            "--depth=1",
        ],
        name="git-sync",
        volumes=[dbt_project_volume],
        volume_mounts=[dbt_project_mount],
        image_pull_policy="IfNotPresent",
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "50m", "memory": "128Mi"},
            limits={"cpu": "250m", "memory": "256Mi"}
        ),
        image_pull_secrets=[k8s.V1LocalObjectReference(name=DBT_IMAGE_PULL_SECRET)],
        is_delete_operator_pod=True,
    )

    # Task 2: dbt run
    run_dbt = KubernetesPodOperator(
        task_id="run_dbt_stg_jobs_model",
        namespace=K8S_NAMESPACE,
        image=DBT_IMAGE,
        cmds=["dbt"],
        arguments=[
            "run",
            "--project-dir", DBT_PROJECT_PATH,
            "--profiles-dir", DBT_PROFILES_PATH,
            "--profile", DBT_PROFILE_NAME,
            "--target", "dev",
            "--select", "stg_jobs"
        ],
        volumes=[dbt_project_volume, dbt_profiles_volume],
        volume_mounts=[dbt_project_mount, dbt_profiles_mount],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "200m", "memory": "512Mi"},
            limits={"cpu": "1000m", "memory": "2Gi"}
        ),
        image_pull_secrets=[k8s.V1LocalObjectReference(name=DBT_IMAGE_PULL_SECRET)],
        is_delete_operator_pod=True,
    )

    # Task 3: dbt test
    test_dbt = KubernetesPodOperator(
        task_id="test_dbt_stg_jobs_model",
        namespace=K8S_NAMESPACE,
        image=DBT_IMAGE,
        cmds=["dbt"],
        arguments=[
            "test",
            "--project-dir", DBT_PROJECT_PATH,
            "--profiles-dir", DBT_PROFILES_PATH,
            "--profile", DBT_PROFILE_NAME,
            "--target", "dev",
            "--select", "stg_jobs"
        ],
        volumes=[dbt_project_volume, dbt_profiles_volume],
        volume_mounts=[dbt_project_mount, dbt_profiles_mount],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "500m", "memory": "1Gi"}
        ),
        image_pull_secrets=[k8s.V1LocalObjectReference(name=DBT_IMAGE_PULL_SECRET)],
        is_delete_operator_pod=True,
    )

    # DAG dependencies
    git_sync_dbt_project >> run_dbt >> test_dbt
