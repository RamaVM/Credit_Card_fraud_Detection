from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# ----------------------------------
# SCHEMA
# ----------------------------------
schema = StructType(
    [StructField("Time", DoubleType(), True)] +
    [StructField(f"V{i}", DoubleType(), True) for i in range(1, 29)] +
    [
        StructField("Amount", DoubleType(), True),
        StructField("Class", IntegerType(), True)
    ]
)

# ----------------------------------
# SPARK SESSION (NO spark.jars!)
# ----------------------------------
spark = (
    SparkSession.builder
    .appName("CreditCardIngestionCleaner")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------
# AWS S3 CONFIG — FIXED
# ----------------------------------
hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()

hadoopConf.set("fs.s3a.access.key", "**********")
hadoopConf.set("fs.s3a.secret.key", "*****")
hadoopConf.set("fs.s3a.endpoint", "*******")
hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 🔥 FORCE SIMPLE CREDENTIALS (avoids IAM error)
hadoopConf.set("fs.s3a.aws.credentials.provider",
               "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# 🔥 JUST IN CASE spark cached IAM provider
hadoopConf.unset("fs.s3a.session.credentials.provider")

# ----------------------------------
# READ RAW KAFKA STREAM
# ----------------------------------
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "creditcard-transactions")
    .option("startingOffsets", "latest")
    .load()
)

df_json = df_raw.selectExpr("CAST(value AS STRING) AS json_str")

df_parsed = df_json.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

df_cleaned = df_parsed.fillna(0).withColumn("ingest_ts", current_timestamp())

# ----------------------------------
# TOGGLE: ENABLE/DISABLE S3 WRITE
# ----------------------------------
ENABLE_S3 = True   # Set True to enable S3 writes, False to disable

# ----------------------------------
# WRITE PARQUET TO S3 (TOGGLE CONTROL)
# ----------------------------------
# ----------------------------------
# WRITE PARQUET TO S3 (TOGGLE CONTROL)
# ----------------------------------
def write_to_s3(batch_df, batch_id):
    if ENABLE_S3:
        print(f"S3 ENABLED - Writing batch {batch_id} to S3...")
        try:
            batch_df.write.format("parquet").mode("append") \
                .save("s3a://credit-card-fraud-detection-1/raw/")
            print(f"Batch {batch_id} successfully written to S3.")
        except Exception as e:
            print("S3 write error:", e)
    else:
        print(f"S3 DISABLED - Skipping S3 write for batch {batch_id}")


batch_query = (
    df_cleaned.writeStream
    .trigger(processingTime="15 seconds")
    .foreachBatch(write_to_s3)
    .outputMode("append")
    .start()
)

# ----------------------------------
# REAL-TIME KAFKA OUT STREAM
# ----------------------------------
df_realtime = df_cleaned.selectExpr("to_json(struct(*)) AS value")

realtime_query = (
    df_realtime.writeStream
    .trigger(processingTime="15 seconds")
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "cleaned-transactions")
    .option("checkpointLocation",
            "file:///D:/data_engineering/fraud_detection/checkpoints/s3_test")
    .outputMode("append")
    .start()
)

batch_query.awaitTermination()
