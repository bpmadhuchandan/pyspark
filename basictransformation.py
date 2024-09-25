from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("PySpark Example").getOrCreate()

# Reading .dat file into DataFrame (assuming it’s a text-based delimited file)
# If it's a space-separated .dat file, use 'sep' to specify the delimiter
df = spark.read.format("csv").option("sep", "|").option("header", "true").load("path_to_file.dat")

# Apply filter transformation on DataFrame (modify the condition as needed)
filtered_df = df.filter(df['column_name'] == "desired_value")

# Creating two tables to demonstrate join
# Assuming you have two DataFrames df1 and df2
df1 = filtered_df.select("column1", "column2")  # Example selection from filtered DataFrame
df2 = spark.read.format("csv").option("sep", "|").option("header", "true").load("path_to_another_file.dat")

# Perform join
joined_df = df1.join(df2, df1["common_column"] == df2["common_column"], "inner")

# Perform exceptAll transformation (finds rows in df1 that aren't in df2)
except_all_df = df1.exceptAll(df2)

# Perform subtract transformation (finds rows in df1 but not in df2)
subtract_df = df1.subtract(df2)

# Writing the output in Append mode (for instance, writing to a CSV file)
except_all_df.write.mode("append").format("csv").save("path_to_output_exceptAll")
subtract_df.write.mode("append").format("csv").save("path_to_output_subtract")

# Stop Spark session
spark.stop()
