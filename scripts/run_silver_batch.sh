#!/bin/bash
set -e

echo "Starting EHR Silver Batch Job..."

SPARK_HOME="${SPARK_HOME:-/spark}"
PIPELINE_HOME="${PIPELINE_HOME:-/pipeline}"
VENV_PYTHON="/venv/bin/python3"

export MINIO_ENDPOINT="http://host.docker.internal:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export MINIO_BUCKET="spark-data"

export PYSPARK_PYTHON=$VENV_PYTHON
export PYSPARK_DRIVER_PYTHON=$VENV_PYTHON

cd $PIPELINE_HOME

$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --driver-memory 1g \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://host.docker.internal:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  jobs/silver_batch.py

echo "Silver Batch Job Complete"
