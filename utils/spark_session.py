# utils/spark_session.py
# Save to: ~/PycharmProjects/oracle_health_pipeline/utils/spark_session.py

import os
from pyspark.sql import SparkSession

# ── MinIO credentials (edit these to match your setup) ───────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",     "spark-data")

# ─────────────────────────────────────────────────────────────────────────────
# THE FIX — these 3 packages override the broken bundled hadoop-aws 3.3.6
# that ships with Spark 4.0.  Must be set BEFORE .getOrCreate()
# ─────────────────────────────────────────────────────────────────────────────
ALL_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.4.1",           # ← replaces bundled 3.3.6
    "com.amazonaws:aws-java-sdk-bundle:1.12.772",   # ← matching AWS SDK
    "io.delta:delta-spark_2.13:4.0.0",              # ← Delta Lake for Spark 4.0
])


def get_spark(app_name: str = "EHR_Pipeline",
              enable_minio: bool = True,
              enable_delta: bool = True,
              streaming: bool = False) -> SparkSession:

    os.environ["SPARK_HOME"] = os.getenv(
        "SPARK_HOME",
        "/Users/subbu/Documents/spark-4.0.0-bin-hadoop3"   # ← your Spark path
    )

    builder = (
        SparkSession.builder
        .appName(app_name)

        # ══════════════════════════════════════════════════════════════════
        # FIX LINES — do not remove these three configs
        # ══════════════════════════════════════════════════════════════════

        # Delta Lake extensions
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Performance
        .config("spark.sql.adaptive.enabled",                       "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",    "true")
        .config("spark.sql.shuffle.partitions",                     "8")
        .config("spark.sql.session.timeZone",                       "UTC")
    )

    if enable_minio:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")   # required for MinIO
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled",    "false")
            .config("spark.hadoop.fs.s3a.connection.maximum",        "100")
            .config("spark.hadoop.fs.s3a.fast.upload",               "true")
            .config("spark.hadoop.fs.s3a.fast.upload.buffer",        "bytebuffer")
            .config("spark.hadoop.fs.s3a.multipart.size",            "67108864")
            .config("spark.hadoop.fs.s3a.attempts.maximum",          "3")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
            .config("spark.hadoop.fs.s3a.connection.timeout",        "10000")
        )

    if streaming:
        builder = (
            builder
            .config("spark.streaming.stopGracefullyOnShutdown",  "true")
            .config("spark.sql.streaming.checkpointLocation",    "/tmp/ehr/checkpoints")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ SparkSession '{app_name}' ready (Spark {spark.version})")
    return spark


# ── Path helpers ──────────────────────────────────────────────────────────────
def s3a(path: str) -> str:
    return f"s3a://{MINIO_BUCKET}/{path.lstrip('/')}"

BRONZE_BASE  = s3a("ehr/bronze")
SILVER_BASE  = s3a("ehr/silver")
GOLD_BASE    = s3a("ehr/gold")
LOCAL_BRONZE = "/tmp/ehr/bronze"
LOCAL_SILVER = "/tmp/ehr/silver"
LOCAL_GOLD   = "/tmp/ehr/gold"