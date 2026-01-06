# producer_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = dict(retries=0)

with DAG(
    dag_id="producer_service",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["ingestion","producer"],
) as dag:

    # EDIT: choose the command that starts your producer and keeps it running
    # Examples:
    # Linux venv example:
    # CMD = "bash -c 'cd /workspace/1_data_ingestion && source venv/bin/activate && python producers/producer.py'"
    # If you're running producer in a docker container via compose, start via compose via docker_ingestion_compose dag
    CMD = "bash -c 'cd /workspace/1_data_ingestion && source venv/bin/activate && python producers/producer.py'"

    start_producer = BashOperator(
        task_id="start_producer_foreground",
        bash_command=CMD,
        retries=0,
        # If you want it to run detached, you can change to run via docker-compose up producer
    )
