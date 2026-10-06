from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "akhil",
    "retries": 0,
}

with DAG(
    dag_id="daily_summary_dag",
    default_args=default_args,
    description="Triggers the Gold daily summary Spark job",
    schedule=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=["payments-pipeline", "gold"],
) as dag:

    run_daily_summary = BashOperator(
        task_id="run_daily_summary_job",
        bash_command=(
            "docker exec -e PYTHONPATH=/opt/app payments-spark "
            "/opt/spark/bin/spark-submit "
            "--packages io.delta:delta-spark_2.12:3.2.0 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "/opt/app/src/streaming/gold_daily_summary.py"
        ),
    )