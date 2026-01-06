from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import shutil

default_args = {"retries": 1}

def delete_folder_contents(path, **kwargs):
    print("Cleaning folder contents:", path)
    if os.path.exists(path):
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)  # delete file or symlink
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # delete folder
            except Exception as e:
                print(f"Failed to delete {file_path}. Reason: {e}")
        print("Folder contents deleted successfully")
    else:
        print("Folder not found:", path)

with DAG(
    dag_id="delete_local_s3_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["infra"],
) as dag:

    t_delete = PythonOperator(
        task_id="delete_local_prefix",
        python_callable=delete_folder_contents,
        op_kwargs={"path": "/opt/airflow/s3_test"},
    )
