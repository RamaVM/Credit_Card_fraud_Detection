# clean_stream_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = dict(retries=1)

with DAG(
    dag_id="clean_stream_job",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["stream","spark"],
) as dag:

    # EDIT THIS: command that works on your machine/container.
    # If Spark is installed on the host and accessible from container, you can:
    # CMD = "spark-submit /workspace/1_data_ingestion/spark_jobs/clean_stream.py"
    # If spark must be run inside host virtualenv or Windows: adapt accordingly.
    CMD = "bash -c 'spark-submit /workspace/1_data_ingestion/spark_jobs/clean_stream.py'"

    t_run = BashOperator(
        task_id="run_clean_stream",
        bash_command=CMD,
        env={"PYTHONPATH": "/workspace"},
    )
