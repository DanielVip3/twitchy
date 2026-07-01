from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession

load_dotenv()

# Initialize Spark session
spark = SparkSession.builder \
  .appName("DotTurinStreamingConsumer") \
  .master("spark://spark-master:7077") \
  .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
  .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ROOT_USER")) \
  .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_ROOT_PASSWORD")) \
  .config("spark.hadoop.fs.s3a.path.style.access", "true") \
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
  .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Flow from Kafka to Spark
kafka_df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:29092") \
  .option("subscribe", "dotturin-bike-status") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false") \
  .load()

readable_df = kafka_df.selectExpr("CAST(value AS STRING) as json_payload", "timestamp")

print("[*] Setup complete. Waiting for data...")

# Write stream in Parquet format in bucket 'dotturin-raw'
query = readable_df.writeStream \
  .format("parquet") \
  .option("path", "s3a://dotturin-raw/bikes/") \
  .option("checkpointLocation", "s3a://dotturin-raw/checkpoints/") \
  .outputMode("append") \
  .start()

query.awaitTermination()