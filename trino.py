from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from pendulum import datetime

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=50, 
    tags=["trino", "distributed", "bigdata"]
)
def trino_distributed_copy():

    TOTAL_RECORDS = 100_000_000
    PARTITION_SIZE = 1_000_000

    @task
    def generate_partitions():
        return [
            {"start_id": i, "end_id": min(i + PARTITION_SIZE - 1, TOTAL_RECORDS)}
            for i in range(1, TOTAL_RECORDS + 1, PARTITION_SIZE)
        ]

    def generate_sql(start_id: int, end_id: int) -> str:
        return f"""
            INSERT INTO target_db.target_table
            SELECT * FROM source_db.source_table
            WHERE id BETWEEN {start_id} AND {end_id}
        """

    @task
    def build_sqls(partitions: list[dict]):
        return [generate_sql(p["start_id"], p["end_id"]) for p in partitions]

    copy_partition = TrinoOperator.partial(
        task_id="copy_partition",
        trino_conn_id="trino_target",
        handler=None,
        poll_interval=30,
        retries=3,
        retry_delay=60,
    ).expand(sql=build_sqls(generate_partitions()))

dag = trino_distributed_copy()
