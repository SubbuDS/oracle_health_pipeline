import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.spark_session import get_spark, BRONZE_BASE, LOCAL_BRONZE, SILVER_BASE, LOCAL_SILVER


# ── PATIENTS ──────────────────────────────────────────────────────────────────
def silver_patients(spark, path):
    df = spark.read.parquet(path)
    df = df.select(
        "patient_id", "mrn", "date_of_birth", "gender",
        "insurance_id", "created_at", "updated_at", "pipeline_ts"
    ).filter(F.col("patient_id").isNotNull())

    if "__op" in df.columns:
        df = df.withColumnRenamed("__op", "cdc_op")
    else:
        df = df.withColumn("cdc_op", F.lit("c"))

    df = (df
        .withColumn("mrn_hash", F.md5(F.col("mrn")).substr(1, 16))
        .withColumn("date_of_birth_dt",
            F.when(
                F.expr("try_cast(date_of_birth as int)").isNotNull(),
                F.expr("date_add(to_date('1970-01-01'), try_cast(date_of_birth as int))")
            ).otherwise(
                F.to_date(F.col("date_of_birth"))
            ))
        .withColumn("age_years",
            F.floor(F.datediff(F.current_date(), "date_of_birth_dt") / 365.25))
        .withColumn("age_group",
            F.when(F.col("age_years") < 18, "PEDIATRIC")
             .when(F.col("age_years") < 65, "ADULT")
             .otherwise("SENIOR"))
        .withColumn("gender",
            F.when(F.upper("gender").isin("M", "MALE"),   "M")
             .when(F.upper("gender").isin("F", "FEMALE"), "F")
             .otherwise("U"))
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("patient_id")
                  .orderBy(F.col("pipeline_ts").desc())))
        .filter(F.col("_rn") == 1).drop("_rn")
        .withColumn("dq_complete",
            F.col("patient_id").isNotNull() & F.col("gender").isin("M", "F"))
        .withColumn("silver_ts", F.current_timestamp())
    )
    return df


# ── ADT EVENTS ────────────────────────────────────────────────────────────────
def silver_adt(spark, path):
    df = spark.read.parquet(path)
    df = df.select(
        "patient_id", "event_id", "event_type", "event_timestamp",
        "facility_id", "ward", "bed_number", "attending_doctor",
        "reason", "is_icu", "created_at", "pipeline_ts"
    ).filter(F.col("event_id").isNotNull())

    if "__op" in df.columns:
        df = df.withColumnRenamed("__op", "cdc_op")
    else:
        df = df.withColumn("cdc_op", F.lit("c"))

    df = (df
        .withColumn("event_time",
            F.to_timestamp(F.col("event_timestamp").cast("long") / 1e6))
        .withColumn("created_at_ts",
            F.to_timestamp(F.col("created_at").cast("long") / 1e6))
        .withColumn("event_type", F.upper("event_type"))
        .withColumn("is_icu", F.col("is_icu").cast("boolean"))
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("event_id")
                  .orderBy(F.col("pipeline_ts").desc())))
        .filter(F.col("_rn") == 1).drop("_rn")
        .withColumn("silver_ts", F.current_timestamp())
    )
    return df


# ── LAB RESULTS ───────────────────────────────────────────────────────────────
def silver_labs(spark, path):
    df = spark.read.parquet(path)
    df = df.select(
        "patient_id", "result_id", "test_code", "test_name",
        "result_value", "unit", "abnormal_flag",
        "result_numeric", "is_critical", "created_at", "pipeline_ts"
    ).filter(F.col("result_id").isNotNull())

    if "__op" in df.columns:
        df = df.withColumnRenamed("__op", "cdc_op")
    else:
        df = df.withColumn("cdc_op", F.lit("c"))

    df = (df
        .withColumn("result_time",
            F.to_timestamp(F.col("created_at").cast("long") / 1e6))
        .withColumn("abnormal_direction",
            F.when(F.upper("abnormal_flag") == "H", "HIGH")
             .when(F.upper("abnormal_flag") == "L", "LOW")
             .otherwise("NORMAL"))
        .withColumn("is_abnormal", F.col("abnormal_flag").isin("H", "L"))
        .withColumn("is_critical", F.col("is_critical").cast("boolean"))
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("result_id")
                  .orderBy(F.col("pipeline_ts").desc())))
        .filter(F.col("_rn") == 1).drop("_rn")
        .withColumn("silver_ts", F.current_timestamp())
    )
    return df


# ── VITALS ────────────────────────────────────────────────────────────────────
VITAL_BOUNDS = {
    "systolic_bp":  (60,  250),
    "diastolic_bp": (30,  150),
    "heart_rate":   (30,  250),
    "spo2":         (50,  100),
    "temperature":  (90,  110),
}

def silver_vitals(spark, path):
    df = spark.read.parquet(path)
    df = df.select(
        "patient_id", "vital_id", "heart_rate", "systolic_bp",
        "diastolic_bp", "spo2", "temperature",
        "created_at", "pipeline_ts"
    ).filter(F.col("vital_id").isNotNull())

    if "__op" in df.columns:
        df = df.withColumnRenamed("__op", "cdc_op")
    else:
        df = df.withColumn("cdc_op", F.lit("c"))

    for c in ["heart_rate", "systolic_bp", "diastolic_bp", "spo2", "temperature"]:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))

    df = (df
        .withColumn("recorded_at",
            F.to_timestamp(F.col("created_at").cast("long") / 1e6))
        .withColumn("news_spo2_flag",
            F.when(F.col("spo2") <= 91, 3)
             .when(F.col("spo2") <= 93, 2)
             .when(F.col("spo2") <= 95, 1)
             .otherwise(0).cast(IntegerType()))
        .withColumn("news_hr_flag",
            F.when(F.col("heart_rate") <= 40,  3)
             .when(F.col("heart_rate") <= 50,  1)
             .when(F.col("heart_rate") <= 90,  0)
             .when(F.col("heart_rate") <= 110, 1)
             .when(F.col("heart_rate") <= 130, 2)
             .otherwise(3).cast(IntegerType()))
        .withColumn("news2_score",
            F.col("news_spo2_flag") + F.col("news_hr_flag"))
        .withColumn("news2_risk",
            F.when(F.col("news2_score") >= 5, "HIGH")
             .when(F.col("news2_score") >= 3, "MEDIUM")
             .otherwise("LOW"))
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("vital_id")
                  .orderBy(F.col("pipeline_ts").desc())))
        .filter(F.col("_rn") == 1).drop("_rn")
        .withColumn("silver_ts", F.current_timestamp())
    )

    for col_name, (lo, hi) in VITAL_BOUNDS.items():
        df = df.withColumn(col_name,
            F.when((F.col(col_name) >= lo) & (F.col(col_name) <= hi),
                   F.col(col_name)))
    return df


# ── WRITER ────────────────────────────────────────────────────────────────────
def write_silver(spark, df, silver_path, local_path, key):
    for path in [silver_path, local_path]:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                DeltaTable.forPath(spark, path).alias("t") \
                    .merge(df.alias("s"), f"t.{key}=s.{key}") \
                    .whenMatchedDelete(condition="s.cdc_op = 'd'") \
                    .whenMatchedUpdateAll(condition="s.cdc_op != 'd'") \
                    .whenNotMatchedInsertAll(condition="s.cdc_op != 'd'").execute()
                print(f"  MERGE  -> {path}")
            else:
                df.write.format("delta").mode("overwrite").save(path)
                print(f"  CREATE -> {path}")
            return
        except Exception as e:
            print(f"  failed {path}: {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run():
    spark = get_spark("EHR_Silver_Batch", enable_minio=True, enable_delta=True)

    jobs = [
        ("patients",    silver_patients, "patient_id"),
        ("adt_events",  silver_adt,      "event_id"),
        ("lab_results", silver_labs,     "result_id"),
        ("vitals",      silver_vitals,   "vital_id"),
    ]

    for table, fn, key in jobs:
        print(f"\n-- {table} --")
        try:
            df = fn(spark, f"{BRONZE_BASE}/{table}")
            count = df.count()
            print(f"  rows={count}")
            write_silver(spark, df,
                         f"{SILVER_BASE}/{table}",
                         f"{LOCAL_SILVER}/{table}", key)
        except Exception as e:
            print(f"  FAILED: {e}")

    spark.stop()
    print("\nSilver batch complete")

if __name__ == "__main__":
    run()