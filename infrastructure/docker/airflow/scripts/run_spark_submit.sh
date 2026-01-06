#!/usr/bin/env bash
# Wrapper to run spark-submit from host environment
# Usage: ./run_spark_submit.sh /workspace/1_data_ingestion/spark_jobs/clean_stream.py

SPARK_APP="$1"
CHECKPOINT_DIR="$2"   # optional

if [ -z "$SPARK_APP" ]; then
  echo "Usage: $0 <path-to-py>"
  exit 1
fi

# Example spark-submit command - adjust spark-home or spark-submit path as needed
# If you have local Spark installed, ensure SPARK_HOME/bin is on PATH
spark-submit \
  --master local[*] \
  --conf "spark.driver.host=127.0.0.1" \
  --conf "spark.driver.bindAddress=0.0.0.0" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
  "$SPARK_APP"
