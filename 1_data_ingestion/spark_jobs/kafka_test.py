from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("jar-check") \
    .getOrCreate()

sc = spark.sparkContext
jars = sc._jsc.sc().listJars().iterator()

print("\n===== JARS LOADED BY SPARK =====\n")

while jars.hasNext():
    print(jars.next())

spark.stop()
