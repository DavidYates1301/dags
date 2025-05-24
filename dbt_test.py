from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

# --- Cấu hình Chung cho dbt Project và Git-Sync ---
# Thay thế các giá trị này bằng thông tin thực tế của bạn
DBT_REPO_URL = "https://github.com/DavidYates1301/dbt_project.git"  # <--- THAY THẾ BẰNG URL REPO GIT CỦA DBT PROJECT CỦA BẠN
DBT_REPO_BRANCH = "main"                                     # <--- THAY THẾ NẾU BẠN DÙNG BRANCH KHÁC (VD: 'dev', 'release')
DBT_PROJECT_CLONE_PATH = "/dbt"                              # Đường dẫn nơi dbt project sẽ được clone trong Pod
DBT_PROFILES_MOUNT_PATH = "/opt/airflow/dbt_profiles"        # Đường dẫn nơi profiles.yml sẽ được mount từ Secret
DBT_PROFILE_NAME = "dbt_scheduler"                          # Tên profile bạn đã định nghĩa trong profiles.yml
DBT_RUNNER_IMAGE = "192.168.1.67:9082/dbt-runner:1.7.0"         # <--- THAY THẾ BẰNG ĐỊA CHỈ DOCKER IMAGE DBT CỦA BẠN
K8S_NAMESPACE = "cd-scheduler"                                    # Namespace nơi Airflow và các tài nguyên Kubernetes của bạn chạy

DBT_RESOURCE_REQUESTS = {"cpu": "200m", "memory": "512Mi"}
DBT_RESOURCE_LIMITS = {"cpu": "1000m", "memory": "2Gi"}

# --- Định nghĩa DAG ---
with DAG(
    dag_id='dbt_postgres_kubernetes_example',
    start_date=datetime(2023, 1, 1), # Ngày bắt đầu lịch sử cho DAG
    schedule_interval=timedelta(days=1), # Chạy mỗi ngày (hoặc None nếu bạn muốn chạy thủ công)
    catchup=False, # Không chạy lại các lượt chạy đã bỏ lỡ
    tags=['dbt', 'kubernetes', 'postgres', 'data_transformation'],
    description='A DAG to run dbt transformations for PostgreSQL jobs data on Kubernetes.',
) as dag:

    # 1. Định nghĩa Volume chung cho dbt project
    #    EmptyDir volume: Dữ liệu tồn tại trong suốt vòng đời của Pod và bị xóa khi Pod kết thúc.
    dbt_project_volume = k8s.V1Volume(name="dbt-project-volume", empty_dir=k8s.V1EmptyDirVolumeSource())
    # Định nghĩa VolumeMount cho dbt project
    dbt_project_volume_mount = k8s.V1VolumeMount(name="dbt-project-volume", mount_path=DBT_PROJECT_CLONE_PATH)

    # 2. Định nghĩa Volume cho dbt profiles (từ Kubernetes Secret)
    dbt_profiles_volume = k8s.V1Volume(
        name="dbt-profiles-volume",
        secret=k8s.V1SecretVolumeSource(secret_name="dbt-profiles") # Tên Secret bạn đã tạo
    )
    # Định nghĩa VolumeMount cho dbt profiles
    dbt_profiles_volume_mount = k8s.V1VolumeMount(
        name="dbt-profiles-volume",
        mount_path=DBT_PROFILES_MOUNT_PATH,
        read_only=True # KHÔNG CHO PHÉP ghi vào thư mục profiles để đảm bảo an toàn
    )

    # 3. Định nghĩa Init Container cho Git-Sync
    #    Container này sẽ chạy trước container chính của task để clone dbt project.
    dbt_git_sync_init_container = k8s.V1Container(
        name="git-sync-dbt-project",
        image="registry.k8s.io/git-sync/git-sync:v4.4.0", # Sử dụng phiên bản git-sync ổn định
        args=[
            f"--repo={DBT_REPO_URL}",
            f"--branch={DBT_REPO_BRANCH}",
            f"--root={DBT_PROJECT_CLONE_PATH}", # Nơi git-sync sẽ clone repo vào trong volume
            "--one-time", # Rất quan trọng: Chỉ clone một lần rồi thoát
            # Nếu repo của bạn là private, bạn cần thêm cấu hình xác thực ở đây (ví dụ: SSH key).
            # Xem lại hướng dẫn chi tiết về "Xử lý Repo Git Private" nếu cần.
        ],
        volume_mounts=[dbt_project_volume_mount], # Git-sync cần mount volume để ghi dữ liệu vào
        security_context=k8s.V1SecurityContext(run_as_user=65532), # Chạy với user nobody để tăng cường bảo mật
    )

    # 4. Task để chạy dbt model 'stg_jobs'
    run_stg_jobs_model = KubernetesPodOperator(
        task_id="run_dbt_stg_jobs_model",
        namespace=K8S_NAMESPACE,
        image=DBT_RUNNER_IMAGE,
        cmds=["dbt"], # Lệnh chính để thực thi dbt
        arguments=[
            "run",
            "--project-dir", DBT_PROJECT_CLONE_PATH,     # Đường dẫn dbt project đã được clone
            "--profiles-dir", DBT_PROFILES_MOUNT_PATH,   # Đường dẫn profiles.yml đã được mount
            "--profile", DBT_PROFILE_NAME,               # Tên profile dbt sẽ sử dụng
            "--target", "dev",                           # Target môi trường dbt sẽ sử dụng
            "--select", "stg_jobs"                       # Chỉ chạy model 'stg_jobs'
        ],
        volumes=[dbt_project_volume, dbt_profiles_volume],       # Khai báo tất cả các Volumes
        volume_mounts=[dbt_project_volume_mount, dbt_profiles_volume_mount], # Khai báo tất cả các VolumeMounts
        init_containers=[dbt_git_sync_init_container],          # Gắn init container vào đây
        do_xcom_push=False,      # Đặt là False nếu bạn không cần truyền dữ liệu giữa các task qua XComs
        is_delete_operator_pod=True, # Đảm bảo Pod bị xóa sau khi task hoàn thành để tiết kiệm tài nguyên
        resources=k8s.V1ResourceRequirements(
            requests=DBT_RESOURCE_REQUESTS,
            limits=DBT_RESOURCE_LIMITS
        ),
        # Nếu bạn dùng Workload Identity hoặc Service Account để cấp quyền database
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
            "--select", "stg_jobs" # Chỉ chạy test cho model 'stg_jobs'
        ],
        volumes=[dbt_project_volume, dbt_profiles_volume],
        volume_mounts=[dbt_project_volume_mount, dbt_profiles_volume_mount],
        init_containers=[dbt_git_sync_init_container], # Cũng cần init container cho task test
        do_xcom_push=False,
        is_delete_operator_pod=True,
        resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"}, # Test thường ít tài nguyên hơn run
            limits={"cpu": "500m", "memory": "1Gi"}
        ),
        # service_account_name="your-kubernetes-service-account-for-dbt",
    )

    # 6. Định nghĩa luồng công việc: Chạy model rồi sau đó chạy test
    run_stg_jobs_model >> test_stg_jobs_model
