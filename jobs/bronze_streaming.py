import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQueryListener
from utils.spark_session import get_spark, BRONZE_BASE

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
CLICKHOUSE_URL   = "http://localhost:8123/?user=ehr_admin&password=ehr2026"

# ── ClickHouse helper ─────────────────────────────────────────────────────────
def ch_write_spark_metrics(rows):
    if not rows:
        return
    values = ", ".join(rows)
    query  = f"INSERT INTO pipeline_metrics.spark_metrics VALUES {values}"
    try:
        r = requests.post(CLICKHOUSE_URL, data=query.encode(), timeout=5)
        if r.status_code != 200:
            print(f"  ClickHouse spark metrics error: {r.text[:200]}")
    except Exception as e:
        print(f"  ClickHouse spark metrics failed: {e}")


# ── Streaming metrics listener ────────────────────────────────────────────────
class SparkMetricsListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p        = event.progress
        ts       = datetime.utcnow().strftime("'%Y-%m-%d %H:%M:%S'")
        job      = p.name or 'bronze_streaming'
        bid      = p.batchId
        inp_rps  = p.inputRowsPerSecond     or 0
        proc_rps = p.processedRowsPerSecond or 0
        num_rows = p.numInputRows           or 0
        trig_ms  = p.durationMs.get("triggerExecution", 0) if p.durationMs else 0

        rows = [
            f"({ts}, '{job}', 'active',                  1,         {bid})",
            f"({ts}, '{job}', 'input_rows_per_sec',    {inp_rps},   {bid})",
            f"({ts}, '{job}', 'processed_rows_per_sec',{proc_rps},  {bid})",
            f"({ts}, '{job}', 'num_input_rows',         {num_rows}, {bid})",
            f"({ts}, '{job}', 'trigger_ms',             {trig_ms},  {bid})",
        ]
        ch_write_spark_metrics(rows)
        print(f"  [metrics] batch={bid} rows={num_rows} "
              f"rps={inp_rps:.1f} trigger={trig_ms}ms -> ClickHouse")

    def onQueryTerminated(self, event):
        ts = datetime.utcnow().strftime("'%Y-%m-%d %H:%M:%S'")
        ch_write_spark_metrics([f"({ts}, 'bronze_streaming', 'active', 0, 0)"])
        print("  [metrics] bronze_streaming terminated -> ClickHouse")


# ── Schemas ───────────────────────────────────────────────────────────────────
PATIENT_SCHEMA = StructType([
    StructField("patient_id",   IntegerType()),
    StructField("mrn",          StringType()),
    StructField("first_name",   StringType()),
    StructField("last_name",    StringType()),
    StructField("date_of_birth",StringType()),
    StructField("gender",       StringType()),
    StructField("ssn",          StringType()),
    StructField("phone",        StringType()),
    StructField("email",        StringType()),
    StructField("address",      StringType()),
    StructField("insurance_id", StringType()),
    StructField("created_at",   StringType()),
    StructField("updated_at",   StringType()),
    StructField("__deleted",    StringType()),
])

ADT_SCHEMA = StructType([
    StructField("event_id",         IntegerType()),
    StructField("patient_id",       IntegerType()),
    StructField("event_type",       StringType()),
    StructField("event_timestamp",  LongType()),
    StructField("facility_id",      StringType()),
    StructField("ward",             StringType()),
    StructField("bed_number",       StringType()),
    StructField("attending_doctor", StringType()),
    StructField("reason",           StringType()),
    StructField("is_icu",           BooleanType()),
    StructField("created_at",       LongType()),
    StructField("__deleted",        StringType()),
])

LAB_SCHEMA = StructType([
    StructField("result_id",     IntegerType()),
    StructField("patient_id",    IntegerType()),
    StructField("test_code",     StringType()),
    StructField("test_name",     StringType()),
    StructField("result_value",  StringType()),
    StructField("unit",          StringType()),
    StructField("abnormal_flag", StringType()),
    StructField("is_critical",   BooleanType()),
    StructField("created_at",    LongType()),
    StructField("__deleted",     StringType()),
])

VITALS_SCHEMA = StructType([
    StructField("vital_id",    IntegerType()),
    StructField("patient_id",  IntegerType()),
    StructField("heart_rate",  IntegerType()),
    StructField("systolic_bp", IntegerType()),
    StructField("diastolic_bp",IntegerType()),
    StructField("spo2",        IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("alert_flags", StringType()),
    StructField("has_alerts",  BooleanType()),
    StructField("created_at",  LongType()),
    StructField("__deleted",   StringType()),
])

TOPICS = {
    "ehr.public.patients":    ("patients",    PATIENT_SCHEMA),
    "ehr.public.adt_events":  ("adt_events",  ADT_SCHEMA),
    "ehr.public.lab_results": ("lab_results", LAB_SCHEMA),
    "ehr.public.vitals":      ("vitals",      VITALS_SCHEMA),
}


# ── Batch writer ──────────────────────────────────────────────────────────────
def write_batch(batch_df, batch_id, table_name, schema):
    if batch_df.rdd.isEmpty():
        return

    try:
        flat_df = (
            batch_df
            .withColumn("row", F.from_json(F.col("raw_value"), schema))
            .select(
                "row.*",
                F.col("kafka_timestamp"),
                F.current_timestamp().alias("pipeline_ts"),
            )
            .filter(F.col("__deleted") != "true")
            .filter(F.col("patient_id").isNotNull())
            .drop("__deleted")
        )
        count = flat_df.count()
        if count == 0:
            return
        print(f"  batch={batch_id} table={table_name} rows={count}")
    except Exception as e:
        print(f"  batch={batch_id} parse error: {e}")
        return

    minio_path = f"{BRONZE_BASE}/{table_name}"
    try:
        flat_df.write.format("parquet").mode("append").save(minio_path)
        print(f"    -> {minio_path}")
    except Exception as e:
        print(f"    MinIO FAILED: {e}")


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    spark = get_spark(
        "EHR_Bronze_Streaming",
        enable_minio=True,
        enable_delta=False,
        streaming=True
    )

    # Register metrics listener — fires after every batch automatically
    spark.streams.addListener(SparkMetricsListener())
    print("SparkMetricsListener registered -> writing metrics to ClickHouse")

    queries = []
    for topic, (table_name, schema) in TOPICS.items():
        checkpoint = f"/tmp/ehr/checkpoints/bronze/{table_name}"
        os.makedirs(checkpoint, exist_ok=True)

        stream_df = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", "500")
            .load()
            .selectExpr(
                "CAST(value AS STRING) AS raw_value",
                "timestamp AS kafka_timestamp",
            )
        )

        q = (
            stream_df.writeStream
            .foreachBatch(
                lambda df, bid, tn=table_name, s=schema:
                    write_batch(df, bid, tn, s)
            )
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="30 seconds")
            .start()
        )
        queries.append(q)
        print(f"Started: {topic} -> {BRONZE_BASE}/{table_name}")

    print("\nAll 4 streams running -> MinIO only")
    print("Ctrl-C to stop\n")

    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping...")
        for q in queries:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    run()