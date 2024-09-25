from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("MultiLineDataExample") \
    .getOrCreate()

# 1. Read multi-line JSON file
json_file_path = "/path/to/multiline-json-file.json"

# Load the multi-line JSON data
df = spark.read.option("multiLine", "true").json(json_file_path)

# Show the DataFrame content
df.show(truncate=False)

# Stop the Spark session
spark.stop()
