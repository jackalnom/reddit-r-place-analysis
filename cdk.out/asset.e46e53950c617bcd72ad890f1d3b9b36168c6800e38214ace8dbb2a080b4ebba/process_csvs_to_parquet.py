from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import split, col 
import sys
from awsglue.utils import getResolvedOptions

job_args = getResolvedOptions(sys.argv, [
        'bucket',
       ])
spark = SparkSession.builder \
    .appName("Ungzip and Merge CSVs to Parquet") \
    .getOrCreate()

# Define the schema
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user", StringType(), True),
    StructField("coordinate", StringType(), True),
    StructField("pixel_color", StringType(), True)
])
bucket = job_args['bucket']
# Specify the source directory
source_dir = f"s3://{bucket}/csvs/*.csv.gz"

# Read the gzip compressed CSV files into a DataFrame with an explicit schema
df = spark.read.csv(source_dir, schema=schema, header=True)

# Filter rows where 'coordinate' is in the format 'x,y'
df = df.filter("coordinate RLIKE '^[0-9]+,[0-9]+$'")

# Split the 'coordinate' field into two separate columns 'x' and 'y', and convert to integers
split_col = split(df['coordinate'], ',')
df = df.withColumn('x', split_col.getItem(0).cast(IntegerType()))
df = df.withColumn('y', split_col.getItem(1).cast(IntegerType()))

# Optionally, drop the original 'coordinate' column if it's no longer needed
df = df.drop("coordinate")

# Drop the 'user' column from the DataFrame
df = df.drop("user")

# Specify the target directory
target_dir = f"s3://{bucket}/processed/"

df.write.mode("overwrite").parquet(target_dir)

# Stop the Spark session
spark.stop()
