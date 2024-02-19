from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.ml.feature import StringIndexer
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
source_dir = "s3://{bucket}/csvs/*.csv.gzip"

# Read the gzip compressed CSV files into a DataFrame with an explicit schema
df = spark.read.option("compression", "gzip").csv(source_dir, schema=schema, header=True)

print(f"SCHEMA: '{df.schema}'")

# Indexing the 'user' field to convert it to an integer index
#stringIndexer = StringIndexer(inputCol="user", outputCol="userIndex")
#model = stringIndexer.fit(df)
#df = model.transform(df)

# Optionally, you can drop the original 'user' column and rename 'userIndex' to 'user'
#df = df.drop("user").withColumnRenamed("userIndex", "user")

# Specify the target directory
target_dir = f"s3://{bucket}/parquet/rplace.parquet"

# Write the DataFrame to a single Parquet file
df.coalesce(1).write.mode("overwrite").parquet(target_dir)

# Stop the Spark session
spark.stop()