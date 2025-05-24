from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

# --- Cấu hình Chung cho dbt Project và Git-Sync ---
DBT_REPO_URL = "https://github.com/DavidYates1301/dbt_project.git"
DBT_REPO_BRANCH = "main"
DBT_PROJECT_CLONE_PATH = "/dbt"
DBT_PROFILES_MOUNT_PATH = "/opt/airflow/dbt_profiles"
DBT_PROFILE_NAME = "dbt_scheduler"
DBT_RUNNER_IMAGE = "192.168.1.67:9082/dbt-runner:1.7.0"
K8S_NAMESPACE = "cd-scheduler"

# Cấu hình tài nguyên (sẽ được dùng để tạo đối tượng V1ResourceRequirements)
DBT_RESOURCE_REQUESTS = {"cpu": "200m", "memory": "512Mi"}
DBT_RESOURCE_LIMITS = {"cpu": "1000m", "memory": "2Gi"}

# --- Định nghĩa DAG ---
with DAG(
    dag_id='dbt_postgres_kubernetes_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=timedelta(days=1), # Giữ nguyên 'schedule_interval'
    catchup=False,
    tags=['dbt', 'kubernetes', 'postgres', 'data_transformation'],
    description='A DAG to run dbt transformations for PostgreSQL jobs data on Kubernetes.',
) as dag:

    dbt_project_volume = k8s.V1Volume(name="dbt-project-volume", empty_dir=k8s.V1EmptyDirVolumeSource())
    dbt_project_volume_mount = k8s.V1VolumeMount(name="dbt-project-volume", mount_path=DBT_PROJECT_CLONE_PATH)

    dbt_profiles_volume = k8s.V1Volume(
        name="dbt-profiles-volume",
        secret=k8s.V1SecretVolumeSource(secret_name="dbt-profiles")
    )
    dbt_profiles_volume_mount = k8s.V1VolumeMount(
        name="dbt-profiles-volume",
        mount_path=DBT_PROFILES_MOUNT_PATH,
        read_only=True
    )

    dbt_git_sync_init_container = k8s.V1Container(
        name="git-sync-dbt-project",
        image="registry.k8s.io/git-sync/git-sync:v4.4.0",
        args=[
            f"--repo={DBT_REPO_URL}",
            f"--branch={DBT_REPO_BRANCH}",
            f"--root={DBT_PROJECT_CLONE_PATH}",
            "--one-time",
        ],
        volume_mounts=[dbt_project_volume_mount],
        security_context=k8s.V1SecurityContext(run_as_user=65532),
    )

    # 4. Task để chạy dbt model 'stg_jobs'
    run_stg_jobs_model = KubernetesPodOperator(
        task_id="run_dbt_stg_jobs_model",
        namespace=K8S_NAMESPACE,
        image=DBT_RUNNER_IMAGE,
        cmds=["dbt"],
        arguments=[
            "run",
            "--project-dir", DBT_PROJECT_CLONE_PATH,
            "--profiles-dir", DBT_PROFILES_MOUNT_PATH,
            "--profile", DBT_PROFILE_NAME,
            "--target", "dev",
            "--select", "stg_jobs"
        ],
        volumes=[dbt_project_volume, dbt_profiles_volume],
        volume_mounts=[dbt_project_volume_mount, dbt_profiles_volume_mount],
        init_containers=[dbt_git_sync_init_container],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        # THAY ĐỔI QUAN TRỌNG NHẤT: Đổi 'resources' thành 'container_resources'
        container_resources=k8s.V1ResourceRequirements(
            requests=DBT_RESOURCE_REQUESTS,
            limits=DBT_RESOURCE_LIMITS
        ),
        # service_account_name="your-kubernetes-service-account-for-dbt",
    )

    # 5. Task để chạy dbt tests cho model 'stg_jobs'
    test_stg_jobs_model = KubernetesPodOperator(
        task_id="test_dbt_stg_jobs_model",
        namespace=K8S_NAMESPACE,
        image=DBT_RUNNER_IMAGE,
        cmds=["dbt"],
        arguments=[
            "test",
            "--project-dir", DBT_PROJECT_CLONE_PATH,
            "--profiles-dir", DBT_PROFILES_MOUNT_PATH,
            "--profile", DBT_PROFILE_NAME,
            "--target", "dev",
            "--select", "stg_jobs"
        ],
        volumes=[dbt_project_volume, dbt_profiles_volume],
        volume_mounts=[dbt_project_volume_mount, dbt_profiles_volume_mount],
        init_containers=[dbt_git_sync_init_container],
        do_xcom_push=False,
        is_delete_operator_pod=True,
        # THAY ĐỔI QUAN TRỌNG NHẤT: Đổi 'resources' thành 'container_resources'
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "500m", "memory": "1Gi"}
        ),
        # service_account_name="your-kubernetes-service-account-for-dbt",
    )

    # 6. Định nghĩa luồng công việc
    run_stg_jobs_model >> test_stg_jobs_model
