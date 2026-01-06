## Real-Time Credit Card Fraud Detection System

An end-to-end, industry-grade real-time fraud detection pipeline built using Kafka, Spark Structured Streaming, Machine Learning, AWS S3, Snowflake, Airflow, Docker, and Streamlit.

This project demonstrates real-world data engineering + streaming ML architecture, from ingestion to analytics and visualization.



## Architecture Overview

Data Flow:

CSV data ingestion

Kafka producer streams transactions + injects synthetic fraud events

Spark Structured Streaming cleans and transforms data

Data flows into two parallel pipelines:

Real-time ML pipeline

Batch analytics pipeline

## Streaming Pipeline (Real-Time)

Kafka streams credit card transactions

Spark cleans and enriches data

LightGBM model predicts fraud vs non-fraud

Outputs:

All transactions → CSV

Fraud transactions → CSV

Real-time dashboard (Streamlit)

Email alerts on fraud detection

## Batch Pipeline (Analytics)

Cleaned data written to AWS S3

Apache Airflow runs daily Snowflake COPY INTO

Snowflake analytics using SQL:

Daily fraud summary

Fraud percentage

High-risk patterns

Hourly fraud trends

Data visualized using Power BI

## Tech Stack
Layer	                    Technologies
Ingestion	                Kafka, Python
Streaming	                Spark Structured Streaming
ML	                        LightGBM
Orchestration	            Apache Airflow
Storage	                    AWS S3
Warehouse	                Snowflake
Visualization	            Streamlit, Power BI
DevOps	                    Docker
Language	                Python

##  Orchestration Strategy (Airflow)

Airflow is used only for batch orchestration

Single DAG:

Runs Snowflake COPY INTO from S3 → Snowflake

Streaming components run continuously and independently

This mirrors real production systems, where streaming is long-running and batch jobs are scheduled.

## Key Highlights

✔ Real-time fraud detection
✔ Streaming + Batch hybrid architecture
✔ Synthetic fraud injection
✔ ML inference on streaming data
✔ Industry-standard tools
✔ Fully containerized
✔ Portfolio-ready project

## Notes

Credentials are removed for security

Environment variables are used

This project is designed for learning + portfolio, not paid production deployment



## Author

Ramakrishna
Aspiring Data Engineer | Streaming & ML Systems
📍 India