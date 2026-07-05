import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType

load_dotenv()

TOPIC_NAME = os.environ.get("TOPIC_NAME")

def get_spark_session(app_name: str, master: str|None = None) -> SparkSession:
  """
  Initialize and return a SparkSession pre-configured for Delta Lake and MinIO.
  """

  spark = SparkSession.builder \
    .appName(app_name) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false") \
    .config("spark.sql.files.ignoreMissingFiles", "true")
  
  if master is not None:
    spark = spark.master(master)
  
  spark = spark.getOrCreate()
      
  spark.sparkContext.setLogLevel("WARN")
  return spark


# Twitch API schema
stream_schema = StructType([
  StructField("id", StringType()),
  StructField("user_name", StringType()),
  StructField("game_name", StringType()),
  StructField("title", StringType()),
  StructField("tags", ArrayType(StringType())),
  StructField("viewer_count", IntegerType()),
  StructField("started_at", TimestampType()),
  StructField("language", StringType()),
  StructField("thumbnail_url", StringType())
])

twitch_api_schema = StructType([
  StructField("data", ArrayType(stream_schema))
])