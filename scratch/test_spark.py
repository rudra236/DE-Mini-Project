import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

print("Python version:", sys.version)

builder = SparkSession.builder \
    .appName("DeltaHelloWorld") \
    .master("local[2]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print("SparkSession created. Version:", spark.version)

# Hello world delta write and read
test_path = "/tmp/delta_hello_world"
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.format("delta").mode("overwrite").save(test_path)
print("Write succeeded.")

read_df = spark.read.format("delta").load(test_path)
read_df.show()
print("Delta Hello World verified!")
spark.stop()
