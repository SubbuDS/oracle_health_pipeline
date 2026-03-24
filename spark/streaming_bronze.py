"""
Its loading data from debezium to bronze folder/layer in minio
Oracle Health Pipeline — Streaming Bronze
Kafka (localhost:29092) → Spark → MinIO (comet bucket) Delta Lake

Your config:
  Kafka:  localhost:29092
  MinIO:  localhost:9000  bucket: comet
  Postgres: localhost:5432 / demo / demo / demo_db
"""

import hashlib
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("OracleHealth.Streaming")

# ── YOUR exact config ──────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:29092"
MINIO_ENDPOINT  = "http://localhost:9000"
MINIO_ACCESS    = "minioadmin"
MINIO_SECRET    = "minioadmin"
BRONZE_BASE     = "s3a://spark-data/ehr/bronze"       # inside your 'comet' bucket

KAFKA_TOPICS = ",".join([
    "ehr.public.patients",
    "ehr.public.adt_events",
    "ehr.public.lab_results",
    "ehr.public.vitals",
])


def build_spark():
    return (
        SparkSession.builder
        .appName("OracleHealth-Streaming-Bronze")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO S3A config
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )


# ── PII masking UDF ────────────────────────────────────────────
def _mask(value):
    if value is None:
        return None
    return hashlib.sha256(value.encode()).hexdigest()[:16]

mask_pii = F.udf(_mask, StringType())


# ── Per-table processors ───────────────────────────────────────

def process_patients(df):
    return (
        df
        .withColumn("mrn_hash",     mask_pii(F.col("mrn")))
        .withColumn("ssn_masked",   mask_pii(F.col("ssn")))
        .withColumn("phone_masked", mask_pii(F.col("phone")))
        .withColumn("email_masked", mask_pii(F.col("email")))
        .drop("ssn", "phone", "email", "address", "first_name", "last_name")
        .withColumn("pipeline_ts", F.current_timestamp())
        .withColumn("source_table", F.lit("patients"))
    )


def process_adt(df):
    return (
        df
        .withColumn("is_icu",
            F.when(
                (F.col("event_type") == "ADMIT") &
                F.upper(F.col("ward")).contains("ICU"), True
            ).otherwise(False))
        .withColumn("pipeline_ts", F.current_timestamp())
        .withColumn("source_table", F.lit("adt_events"))
    )


def process_labs(df):
    CRITICAL = ["TROP", "ABG", "LACT", "K", "NA"]
    return (
        df
        .withColumn("result_numeric", F.col("result_value").cast(DoubleType()))
        .withColumn("is_critical",
            F.col("test_code").isin(CRITICAL) &
            F.col("abnormal_flag").isin("H", "L"))
        .withColumn("pipeline_ts", F.current_timestamp())
        .withColumn("source_table", F.lit("lab_results"))
    )


def process_vitals(df):
    def _flags(hr, spo2, sbp):
        flags = []
        if hr  and hr  > 130: flags.append("TACHYCARDIA")
        if hr  and hr  < 50:  flags.append("BRADYCARDIA")
        if spo2 and spo2 < 90: flags.append("CRITICAL_LOW_SPO2")
        if sbp and sbp > 180: flags.append("HYPERTENSIVE_CRISIS")
        return "|".join(flags) if flags else "NORMAL"

    flag_udf = F.udf(_flags, StringType())
    return (
        df
        .withColumn("alert_flags",
            flag_udf(F.col("heart_rate"), F.col("spo2"), F.col("systolic_bp")))
        .withColumn("has_alerts", F.col("alert_flags") != "NORMAL")
        .withColumn("pipeline_ts", F.current_timestamp())
        .withColumn("source_table", F.lit("vitals"))
    )


PROCESSORS = {
    "ehr.public.patients":    process_patients,
    "ehr.public.adt_events":  process_adt,
    "ehr.public.lab_results": process_labs,
    "ehr.public.vitals":      process_vitals,
}


def get_schema():
    return StructType([
        StructField("patient_id",       IntegerType(), True),
        StructField("mrn",              StringType(),  True),
        StructField("first_name",       StringType(),  True),
        StructField("last_name",        StringType(),  True),
        StructField("date_of_birth",    StringType(),  True),
        StructField("gender",           StringType(),  True),
        StructField("ssn",              StringType(),  True),
        StructField("phone",            StringType(),  True),
        StructField("email",            StringType(),  True),
        StructField("address",          StringType(),  True),
        StructField("insurance_id",     StringType(),  True),
        StructField("event_id",         IntegerType(), True),
        StructField("event_type",       StringType(),  True),
        StructField("event_timestamp",  StringType(),  True),
        StructField("facility_id",      StringType(),  True),
        StructField("ward",             StringType(),  True),
        StructField("bed_number",       StringType(),  True),
        StructField("attending_doctor", StringType(),  True),
        StructField("reason",           StringType(),  True),
        StructField("result_id",        IntegerType(), True),
        StructField("test_code",        StringType(),  True),
        StructField("test_name",        StringType(),  True),
        StructField("result_value",     StringType(),  True),
        StructField("unit",             StringType(),  True),
        StructField("abnormal_flag",    StringType(),  True),
        StructField("vital_id",         IntegerType(), True),
        StructField("heart_rate",       IntegerType(), True),
        StructField("systolic_bp",      IntegerType(), True),
        StructField("diastolic_bp",     IntegerType(), True),
        StructField("spo2",             IntegerType(), True),
        StructField("temperature",      DoubleType(),  True),
        StructField("__op",             StringType(),  True),
        StructField("__table",          StringType(),  True),
        StructField("created_at",       StringType(),  True),
        StructField("updated_at",       StringType(),  True),
    ])


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("="*50)
    logger.info("Oracle Health Streaming Pipeline starting")
    logger.info(f"Kafka:  {KAFKA_BOOTSTRAP}")
    logger.info(f"MinIO:  {MINIO_ENDPOINT}")
    logger.info(f"Bronze: {BRONZE_BASE}")
    logger.info("="*50)

    # Read from all Kafka topics
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON payload
    parsed = (
        raw
        .withColumn("value_str", F.col("value").cast(StringType()))
        .withColumn("topic",     F.col("topic").cast(StringType()))
        .withColumn("kafka_ts",  F.col("timestamp"))
        .withColumn("payload",   F.from_json("value_str", get_schema()))
        .select("payload.*", "topic", "kafka_ts")
    )

    # Start one stream per topic
    queries = []
    for topic, processor in PROCESSORS.items():
        table_name = topic.split(".")[-1]
        out_path   = f"{BRONZE_BASE}/{table_name}/"
        ckpt       = f"/tmp/ehr_checkpoints/bronze_{table_name}"

        stream_df = (
            parsed
            .filter(F.col("topic") == topic)
        )

        processed = processor(stream_df)

        q = (
            processed.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", ckpt)
            .option("mergeSchema", "true")
            .trigger(processingTime="10 seconds")
            .start(out_path)
        )
        queries.append(q)
        logger.info(f"Stream started: {topic} → {out_path}")

    logger.info(f"\nAll {len(queries)} streams running.")
    logger.info("Check MinIO at http://localhost:9000")
    logger.info("Bucket: comet → ehr/bronze/")

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    run()