#!/bin/bash
set -e

echo "Starting EHR Silver Batch Job on Kubernetes..."

SPARK_HOME="${SPARK_HOME:-/spark}"
PIPELINE_HOME="${PIPELINE_HOME:-/pipeline}"

# Get the IP of the machine running the driver (Mac or Jenkins container)
DRIVER_HOST=$(hostname -I | awk '{print $1}')
echo "Driver host: $DRIVER_HOST"

export MINIO_ENDPOINT="http://host.docker.internal:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export MINIO_BUCKET="spark-data"

export PYSPARK_DRIVER_PYTHON=/opt/pyenv/bin/python3
export PYSPARK_PYTHON=/opt/pyenv/bin/python3

$SPARK_HOME/bin/spark-submit \
  --master k8s://https://kubernetes.docker.internal:6443 \
  --deploy-mode client \
  --name ehr-silver-batch \
  --conf spark.kubernetes.namespace=ehr-dev \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.kubernetes.container.image=ehr-spark:latest \
  --conf spark.kubernetes.container.image.pullPolicy=Never \
  --conf spark.driver.host=${DRIVER_HOST} \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.hadoop.fs.s3a.endpoint=http://host.docker.internal:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.executorEnv.MINIO_ENDPOINT=http://host.docker.internal:9000 \
  --conf spark.executorEnv.MINIO_ACCESS_KEY=minioadmin \
  --conf spark.executorEnv.MINIO_SECRET_KEY=minioadmin \
  --conf spark.executorEnv.MINIO_BUCKET=spark-data \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  $PIPELINE_HOME/jobs/silver_batch.py

echo "Silver Batch Job Complete"
