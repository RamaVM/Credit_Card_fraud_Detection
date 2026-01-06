# email_alert_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import smtplib
from email.mime.text import MIMEText

def send_email(subject, body, recipients):
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", 587))
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASSWORD")
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ",".join(recipients)
    s = smtplib.SMTP(smtp_host, smtp_port)
    s.starttls()
    s.login(smtp_user, smtp_pass)
    s.sendmail(smtp_user, recipients, msg.as_string())
    s.quit()

with DAG(
    dag_id="email_alert_service",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    tags=["alert","email"],
) as dag:

    send_alert = PythonOperator(
        task_id="send_test_email",
        python_callable=send_email,
        op_kwargs={
            "subject": "Airflow Alert - test",
            "body": "This is a test from Airflow email_alert_service DAG.",
            "recipients": [os.getenv("SMTP_USER")],
        },
    )
