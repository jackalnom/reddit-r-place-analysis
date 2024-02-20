from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("~/Downloads/2023_place_canvas_history-000000000000.csv.gzip").getOrCreate()

# Read the gzipped CSV file
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("path_to_your_file.csv.gz")

# Print the first 10 rows
df.show(10)