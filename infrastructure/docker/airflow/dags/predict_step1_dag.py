# predict_step1_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = dict(retries=1)

with DAG(
    dag_id="predict_step1",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["predict"],
) as dag:

    # EDIT: run inside venv or container - update path to activate script for Linux or Windows
    CMD = "bash -c 'cd /workspace/1_data_ingestion && source venv/bin/activate && python predict_step1.py'"

    t_predict = BashOperator(
        task_id="run_predict_step1",
        bash_command=CMD,
    )
