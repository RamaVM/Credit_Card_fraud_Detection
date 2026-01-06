# dashboard_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dashboard_task",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["dashboard"],
) as dag:

    # EDIT: command that prepares/generates dashboard or pushes to superset
    CMD = "bash -c 'cd /workspace && python /workspace/infrastructure/scripts/dashboard.py'"

    t_dashboard = BashOperator(
        task_id="run_dashboard_script",
        bash_command=CMD,
    )
