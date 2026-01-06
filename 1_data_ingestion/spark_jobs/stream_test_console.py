from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("s3-test").getOrCreate()
hadoop = spark.sparkContext._jsc.hadoopConfiguration()
hadoop.set("fs.s3a.access.key","AKIAVFIWIXW5YYSZ2TUQ")
hadoop.set("fs.s3a.secret.key","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")
hadoop.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop.set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.write.mode("overwrite").parquet("s3a://credit-card-fraud-detection-1/test/spark_smoke/")
spark.stop()
