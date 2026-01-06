

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import os

SQL_DIR = "/opt/airflow/sql"

SNOWFLAKE_CONN = {
    "user": "ur user name ",
    "password": "********",
    "account": "******",
    "role": "ACCOUNTADMIN",
    "warehouse": "****",
    "database": "MYDB",
    "schema": "PUBLIC"
}

def run_all_sql_files():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONN)
    cs = conn.cursor()

    sql_files = [
        "copy_into.sql",
        "analytics_high_risk_patterns.sql",
        "daily_fraud_summary.sql",
        "fraud_amount_analysis.sql",
        "fraud_hourly_pattern.sql",
        "fraud_percentage_report.sql",
        "top_fraud_transactions.sql"
    ]

    for file in sql_files:
        file_path = os.path.join(SQL_DIR, file)
        with open(file_path, "r") as f:
            sql = f.read()

        print(f"🚀 Executing: {file}")
        cs.execute(sql)

    cs.close()
    conn.close()

with DAG(
    dag_id="snowflake_batch_analytics",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    run_sql = PythonOperator(
        task_id="run_snowflake_sql_files",
        python_callable=run_all_sql_files
    )

    run_sql
