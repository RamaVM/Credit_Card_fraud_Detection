# master_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="master_workflow",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["master"],
) as dag:

    t1 = TriggerDagRunOperator(task_id="trigger_delete_s3", trigger_dag_id="delete_s3_test")
    t2 = TriggerDagRunOperator(task_id="trigger_docker_ingestion", trigger_dag_id="docker_ingestion_compose")
    t3 = TriggerDagRunOperator(task_id="trigger_producer", trigger_dag_id="producer_service")
    t4 = TriggerDagRunOperator(task_id="trigger_clean_stream", trigger_dag_id="clean_stream_job")
    t5 = TriggerDagRunOperator(task_id="trigger_predict", trigger_dag_id="predict_step1")
    t6 = TriggerDagRunOperator(task_id="trigger_dashboard", trigger_dag_id="dashboard_task")
    t7 = TriggerDagRunOperator(task_id="trigger_email_alert", trigger_dag_id="email_alert_service")
    t8 = TriggerDagRunOperator(task_id="trigger_snowflake", trigger_dag_id="snowflake_queries")

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8
