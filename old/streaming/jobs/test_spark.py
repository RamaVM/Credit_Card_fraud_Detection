from pyspark.sql import SparkSession

print("Creating SparkSession...")

spark = SparkSession.builder \
    .appName("TestSpark") \
    .config("spark.driver.extraJavaOptions",
            "-Djava.security.properties=D:/data_engineering/fraud-detection/spark_conf/java.security") \
    .config("spark.executor.extraJavaOptions",
            "-Djava.security.properties=D:/data_engineering/fraud-detection/spark_conf/java.security") \
    .getOrCreate()

print("Spark started successfully!")
spark.stop()
