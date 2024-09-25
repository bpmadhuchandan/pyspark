from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark session configured for Delta Lake
spark = SparkSession.builder \
    .appName("DeltaTableExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Create a sample DataFrame
data = [
    (1, "Alice", 29),
    (2, "Bob", 35),
    (3, "Catherine", 45),
    (4, "David", 33)
]
columns = ["id", "name", "age"]

df = spark.createDataFrame(data, columns)

# 2. Write the DataFrame to a Delta table
delta_table_path = "/tmp/delta-table"  # You can change this to the appropriate path
df.write.format("delta").mode("overwrite").save(delta_table_path)

# 3. Read the data back from the Delta table
delta_df = spark.read.format("delta").load(delta_table_path)
delta_df.show()

# 4. Stop the Spark session
spark.stop()
