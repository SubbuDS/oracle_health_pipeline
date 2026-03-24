
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils.spark_session import get_spark, SILVER_BASE, LOCAL_SILVER, GOLD_BASE, LOCAL_GOLD

# Silver schemas (actual):
# patients:    patient_id, mrn, gender, age_years, age_group, mrn_hash,
#              dq_complete, date_of_birth_dt, silver_ts
# adt_events:  patient_id, event_id, event_type, event_time,
#              facility_id, ward, bed_number, attending_doctor,
#              reason, is_icu, silver_ts
# lab_results: patient_id, result_id, test_code, test_name,
#              result_value, unit, result_numeric, abnormal_flag,
#              abnormal_direction, is_abnormal, is_critical,
#              result_time, silver_ts
# vitals:      patient_id, vital_id, heart_rate, systolic_bp,
#              diastolic_bp, spo2, temperature, alert_flags,
#              has_alerts, recorded_at, news_spo2_flag, news_hr_flag,
#              news2_score, news2_risk, silver_ts


def read_silver(spark, table):
    for path in [f"{SILVER_BASE}/{table}", f"{LOCAL_SILVER}/{table}"]:
        try:
            df = spark.read.format("delta").load(path)
            print(f"  read silver/{table} ({df.count()} rows) from {path}")
            return df
        except Exception as e:
            print(f"  skip {path}: {e}")
    raise RuntimeError(f"Cannot read silver/{table}")


# ── GOLD 1: patient_summary ───────────────────────────────────────────────────

def gold_patient_summary(patients, adt, labs, vitals):
    # latest vitals per patient
    latest_vitals = (
        vitals
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("patient_id").orderBy(F.col("recorded_at").desc())))
        .filter(F.col("_rn") == 1)
        .select("patient_id",
                F.col("systolic_bp").alias("latest_sbp"),
                F.col("diastolic_bp").alias("latest_dbp"),
                F.col("heart_rate").alias("latest_hr"),
                F.col("spo2").alias("latest_spo2"),
                F.col("temperature").alias("latest_temp_f"),
                F.col("news2_risk").alias("latest_news2_risk"),
                F.col("recorded_at").alias("vitals_recorded_at"))
    )

    # encounter summary
    enc = (
        adt.groupBy("patient_id").agg(
            F.count("event_id").alias("total_encounters"),
            F.sum(F.when(F.col("event_type") == "ADMIT", 1).otherwise(0))
             .alias("total_admits"),
            F.max("event_time").alias("last_admit_date"),
            F.sum(F.col("is_icu").cast("int")).alias("icu_admissions"))
    )

    # lab summary last 90 days
    cutoff = F.date_sub(F.current_date(), 90)
    lab_sum = (
        labs.filter(F.to_date("result_time") >= cutoff)
        .groupBy("patient_id").agg(
            F.count("result_id").alias("lab_count_90d"),
            F.sum(F.col("is_abnormal").cast("int")).alias("abnormal_labs_90d"),
            F.sum(F.col("is_critical").cast("int")).alias("critical_labs_90d"))
    )

    return (
        patients
        .join(latest_vitals, "patient_id", "left")
        .join(enc,           "patient_id", "left")
        .join(lab_sum,       "patient_id", "left")
        .select(
            "patient_id", "mrn_hash", "gender", "age_years", "age_group",
            "dq_complete",
            "latest_sbp", "latest_dbp", "latest_hr",
            "latest_spo2", "latest_temp_f", "latest_news2_risk",
            "vitals_recorded_at",
            "total_encounters", "total_admits", "icu_admissions",
            "last_admit_date",
            "lab_count_90d", "abnormal_labs_90d", "critical_labs_90d",
            F.current_timestamp().alias("gold_computed_at"))
    )


# ── GOLD 2: encounter_metrics ─────────────────────────────────────────────────

def gold_encounter_metrics(adt, vitals):
    vital_agg = (
        vitals.groupBy("patient_id").agg(
            F.count("vital_id").alias("vital_count"),
            F.avg("systolic_bp").alias("avg_sbp"),
            F.min("spo2").alias("min_spo2"),
            F.max("heart_rate").alias("max_hr"),
            F.max("news2_score").alias("max_news2_score"))
        .withColumn("news2_risk",
            F.when(F.col("max_news2_score") >= 5, "HIGH")
             .when(F.col("max_news2_score") >= 3, "MEDIUM")
             .otherwise("LOW"))
    )

    return (
        adt
        .join(vital_agg, "patient_id", "left")
        .select(
            "event_id", adt["patient_id"],
            "event_type", "event_time",
            "facility_id", "ward", "bed_number",
            "attending_doctor", "reason", "is_icu",
            "vital_count", "avg_sbp", "min_spo2",
            "max_hr", "max_news2_score", "news2_risk",
            F.current_timestamp().alias("gold_computed_at"))
    )


# ── GOLD 3: daily_census ──────────────────────────────────────────────────────

def gold_daily_census(adt):
    return (
        adt
        .withColumn("event_date", F.to_date("event_time"))
        .groupBy("event_date", "ward", "event_type")
        .agg(F.count("event_id").alias("event_count"))
        .withColumn("gold_computed_at", F.current_timestamp())
        .orderBy("event_date", "ward")
    )


# ── GOLD 4: critical_alerts ───────────────────────────────────────────────────

def gold_critical_alerts(labs, patients):
    cutoff = F.date_sub(F.current_date(), 7)
    return (
        labs
        .filter((F.col("is_critical") == True) &
                (F.to_date("result_time") >= cutoff))
        .join(patients.select(
              "patient_id", "mrn_hash", "age_years", "gender", "dq_complete"),
              "patient_id", "left")
        .select(
            "result_id", "patient_id", "mrn_hash",
            "age_years", "gender",
            "test_name", "test_code",
            "result_numeric", "unit",
            "abnormal_direction", "abnormal_flag",
            "result_time",
            F.current_timestamp().alias("alert_generated_at"))
        .orderBy(F.col("result_time").desc())
    )


# ── GOLD 5: quality_dashboard ─────────────────────────────────────────────────

def gold_quality_dashboard(patients, labs, vitals):
    patient_dq = (
        patients.groupBy(F.to_date("silver_ts").alias("metric_date")).agg(
            F.count("patient_id").alias("patient_count"),
            F.sum(F.col("dq_complete").cast("int")).alias("dq_complete_count"),
            (F.sum(F.col("dq_complete").cast("int")) /
             F.count("patient_id") * 100).alias("dq_completeness_pct"))
    )
    lab_dq = (
        labs.groupBy(F.to_date("result_time").alias("metric_date")).agg(
            F.count("result_id").alias("lab_count"),
            F.sum(F.col("is_abnormal").cast("int")).alias("abnormal_count"),
            F.sum(F.col("is_critical").cast("int")).alias("critical_count"),
            (F.sum(F.col("is_critical").cast("int")) /
             F.count("result_id") * 100).alias("critical_rate_pct"))
    )
    vital_dq = (
        vitals.groupBy(F.to_date("recorded_at").alias("metric_date")).agg(
            F.count("vital_id").alias("vital_count"),
            F.sum(F.col("has_alerts").cast("int")).alias("alert_count"))
    )
    return (
        patient_dq
        .join(lab_dq,   "metric_date", "outer")
        .join(vital_dq, "metric_date", "outer")
        .withColumn("gold_computed_at", F.current_timestamp())
        .orderBy(F.col("metric_date").desc())
    )


# ── WRITER ────────────────────────────────────────────────────────────────────

def write_gold(df, table_name):
    for path in [f"{GOLD_BASE}/{table_name}", f"{LOCAL_GOLD}/{table_name}"]:
        try:
            df.write.format("delta").mode("overwrite") \
              .option("overwriteSchema", "true").save(path)
            print(f"  wrote {table_name} -> {path}  ({df.count()} rows)")
            return
        except Exception as e:
            print(f"  failed {path}: {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run():
    spark = get_spark("EHR_Gold_Batch", enable_minio=True, enable_delta=True)

    print("\n-- Loading Silver --")
    patients = read_silver(spark, "patients")
    adt      = read_silver(spark, "adt_events")
    labs     = read_silver(spark, "lab_results")
    vitals   = read_silver(spark, "vitals")

    print("\n-- Building Gold --")
    gold_tables = [
        ("patient_summary",   gold_patient_summary(patients, adt, labs, vitals)),
        ("encounter_metrics", gold_encounter_metrics(adt, vitals)),
        ("daily_census",      gold_daily_census(adt)),
        ("critical_alerts",   gold_critical_alerts(labs, patients)),
        ("quality_dashboard", gold_quality_dashboard(patients, labs, vitals)),
    ]

    for name, df in gold_tables:
        print(f"\n-- {name} --")
        try:
            write_gold(df, name)
        except Exception as e:
            print(f"  FAILED: {e}")

    spark.stop()
    print("\nGold batch complete")

if __name__ == "__main__":
    run()
