import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Create a GlueContext and SparkContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Extract the AWS Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create a Glue job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the source S3 path and target RDS table
source_path = "s3://your-bucket-name/path/to/csv/file.csv"
target_table = "your_rds_table"

# Read the CSV file from S3
data_frame = spark.read.format("csv").option("header", "true").load(source_path)

# Write the data frame to the target RDS table
data_frame.write.format("jdbc").option("url", "jdbc:postgresql://your-rds-endpoint:5432/your-database").option("dbtable", target_table).mode("append").save()

# Commit the job
job.commit()