FROM apache/airflow:2.7.1
USER root
COPY requirements.txt /requirements/requirements.txt
RUN pip install --no-cache-dir -r /requirements/requirements.txt
USER airflow
