Here’s a complete PySpark program that reads a CSV file, applies several common transformations, and then writes the transformed data back to an Amazon S3 bucket.

Steps in the Code:

1. Read CSV File: Load data from a CSV file.


2. Apply Transformations: Apply a set of transformations such as filtering, selecting, adding new columns, renaming, and grouping data.


3. Write the Result to S3: Write the transformed data into an S3 bucket.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

# Step 1: Create a Spark Session
spark = SparkSession.builder \
    .appName("CSV Transformation Example") \
    .getOrCreate()

# Step 2: Read CSV file
input_file = "s3a://your-bucket-name/input-data.csv"  # Replace with your S3 path
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_file)

# Step 3: Apply Transformations

# Select specific columns
df_selected = df.select("col1", "col2", "col3")  # Replace with actual column names

# Filter rows based on a condition
df_filtered = df_selected.filter(col("col2") > 50)

# Add a new column based on condition
df_with_new_col = df_filtered.withColumn("new_col", when(col("col3") > 100, lit("High")).otherwise(lit("Low")))

# Rename a column
df_renamed = df_with_new_col.withColumnRenamed("col1", "new_col1")

# Group by and aggregation (e.g., sum)
df_grouped = df_renamed.groupBy("new_col").sum("col2")

# Step 4: Write Transformed Data to S3
output_path = "s3a://your-bucket-name/transformed-data"  # Replace with your S3 path
df_grouped.write.mode("overwrite").csv(output_path, header=True)

# Step 5: Stop the Spark session
spark.stop()
Explanation:

SparkSession: Entry point for using Spark. We initialize it using SparkSession.builder().

Read CSV: Use .read.option("header", "true") to load the CSV with the header and automatically infer the schema using inferSchema.

Transformation 1 - Selecting Columns: The .select() method is used to select specific columns from the DataFrame.

Transformation 2 - Filtering: The .filter() method is used to filter rows where the value of col2 is greater than 50.

Transformation 3 - Adding a New Column: The .withColumn() method adds a new column new_col based on the condition.

Transformation 4 - Renaming Columns: The .withColumnRenamed() method renames col1 to new_col1.

Transformation 5 - Aggregation: The .groupBy() method groups data based on a column and applies aggregation using .sum().

Write to S3: The transformed data is written to an S3 path using the .write.csv() method. mode("overwrite") ensures that existing data at the location is overwritten.


Pre-requisites:

1. AWS S3 Configuration: Ensure that your Spark environment has access to AWS credentials. If you're running on EMR or Databricks, credentials might already be configured. If running locally, configure AWS credentials:

export AWS_ACCESS_KEY_ID='your-access-key'
export AWS_SECRET_ACCESS_KEY='your-secret-key'


2. Hadoop Configuration for S3: For PySpark to write to S3, it needs to know the S3A connector. Ensure you have the following dependencies:

Add hadoop-aws and aws-java-sdk jars to your PySpark session.


.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")


3. S3 Path Format: Use the s3a:// prefix when dealing with S3 paths.
