import sys, os, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_session import get_spark, GOLD_BASE, LOCAL_GOLD


def read_gold(spark, table):
    for path in [f"{GOLD_BASE}/{table}", f"{LOCAL_GOLD}/{table}"]:
        try:
            df = spark.read.format("delta").load(path)
            df.createOrReplaceTempView(table)
            return df
        except Exception:
            pass
    raise RuntimeError(f"Gold table {table} not found.")


def banner(title):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def query_patient_summary(spark):
    read_gold(spark, "patient_summary")

    banner("Q1 - Population Overview")
    spark.sql("""
        SELECT COUNT(*)                          AS total_patients,
               ROUND(AVG(age_years), 1)          AS avg_age,
               SUM(CASE WHEN gender='F' THEN 1 END) AS female,
               SUM(CASE WHEN gender='M' THEN 1 END) AS male,
               ROUND(AVG(total_encounters), 1)   AS avg_encounters,
               SUM(icu_admissions)               AS total_icu_admissions,
               SUM(critical_labs_90d)            AS total_critical_labs_90d
        FROM patient_summary
    """).show()

    banner("Q2 - Age Group Breakdown")
    spark.sql("""
        SELECT age_group,
               COUNT(*)                              AS patient_count,
               ROUND(AVG(total_encounters), 1)       AS avg_encounters,
               ROUND(AVG(critical_labs_90d), 2)      AS avg_critical_labs,
               SUM(icu_admissions)                   AS total_icu
        FROM patient_summary
        GROUP BY age_group
        ORDER BY age_group
    """).show()

    banner("Q3 - Top Patients by Critical Labs (last 90 days)")
    spark.sql("""
        SELECT patient_id, mrn_hash, age_years, gender, age_group,
               total_encounters, icu_admissions,
               abnormal_labs_90d, critical_labs_90d,
               latest_sbp, latest_spo2, latest_news2_risk
        FROM patient_summary
        ORDER BY critical_labs_90d DESC, abnormal_labs_90d DESC
        LIMIT 10
    """).show()


def query_encounter_metrics(spark):
    read_gold(spark, "encounter_metrics")

    banner("Q4 - Encounters by Ward")
    spark.sql("""
        SELECT ward,
               COUNT(*)                        AS encounters,
               SUM(CASE WHEN is_icu THEN 1 ELSE 0 END) AS icu_count,
               ROUND(AVG(avg_sbp), 1)          AS avg_sbp,
               ROUND(AVG(min_spo2), 1)         AS avg_min_spo2
        FROM encounter_metrics
        WHERE event_type = 'ADMIT'
        GROUP BY ward
        ORDER BY encounters DESC
    """).show()

    banner("Q5 - NEWS2 Risk Distribution")
    spark.sql("""
        SELECT news2_risk,
               COUNT(*)                    AS encounter_count,
               ROUND(AVG(avg_sbp), 1)      AS avg_sbp,
               ROUND(AVG(min_spo2), 1)     AS avg_min_spo2,
               ROUND(AVG(max_hr), 1)       AS avg_max_hr
        FROM encounter_metrics
        WHERE news2_risk IS NOT NULL
        GROUP BY news2_risk
        ORDER BY CASE news2_risk WHEN 'HIGH' THEN 1
                                 WHEN 'MEDIUM' THEN 2
                                 ELSE 3 END
    """).show()


def query_daily_census(spark):
    read_gold(spark, "daily_census")

    banner("Q6 - Daily Census by Ward")
    spark.sql("""
        SELECT event_date,
               SUM(CASE WHEN event_type='ADMIT'    THEN event_count ELSE 0 END) AS admissions,
               SUM(CASE WHEN event_type='DISCHARGE' THEN event_count ELSE 0 END) AS discharges,
               SUM(CASE WHEN event_type='TRANSFER'  THEN event_count ELSE 0 END) AS transfers,
               SUM(event_count) AS total_events
        FROM daily_census
        GROUP BY event_date
        ORDER BY event_date DESC
        LIMIT 14
    """).show()

    banner("Q7 - Busiest Wards")
    spark.sql("""
        SELECT ward,
               SUM(event_count) AS total_events,
               SUM(CASE WHEN event_type='ADMIT' THEN event_count ELSE 0 END) AS admissions
        FROM daily_census
        GROUP BY ward
        ORDER BY total_events DESC
    """).show()


def query_critical_alerts(spark):
    read_gold(spark, "critical_alerts")

    banner("Q8 - Critical Alerts (last 7 days)")
    spark.sql("""
        SELECT result_id, mrn_hash, age_years, gender,
               test_name, test_code,
               result_numeric, unit,
               abnormal_direction, result_time
        FROM critical_alerts
        ORDER BY result_time DESC
    """).show(20, truncate=False)

    banner("Q9 - Critical Alerts by Test")
    spark.sql("""
        SELECT test_name, test_code,
               COUNT(*)                                        AS alert_count,
               SUM(CASE WHEN abnormal_direction='HIGH' THEN 1 END) AS high_count,
               SUM(CASE WHEN abnormal_direction='LOW'  THEN 1 END) AS low_count,
               ROUND(AVG(result_numeric), 2)                   AS avg_value,
               ROUND(AVG(age_years), 1)                        AS avg_patient_age
        FROM critical_alerts
        GROUP BY test_name, test_code
        ORDER BY alert_count DESC
    """).show()


def query_quality_dashboard(spark):
    read_gold(spark, "quality_dashboard")

    banner("Q10 - Data Quality KPIs")
    spark.sql("""
        SELECT metric_date,
               patient_count,
               dq_complete_count,
               ROUND(dq_completeness_pct, 1)  AS completeness_pct,
               lab_count,
               abnormal_count,
               critical_count,
               ROUND(critical_rate_pct, 2)     AS critical_rate_pct,
               vital_count,
               alert_count
        FROM quality_dashboard
        ORDER BY metric_date DESC
        LIMIT 30
    """).show()


QUERY_MAP = {
    "patient_summary":   query_patient_summary,
    "encounter_metrics": query_encounter_metrics,
    "daily_census":      query_daily_census,
    "critical_alerts":   query_critical_alerts,
    "quality_dashboard": query_quality_dashboard,
}


def run(tables):
    spark = get_spark("EHR_Gold_Query", enable_minio=True, enable_delta=True)
    for table in tables:
        if table not in QUERY_MAP:
            print(f"Unknown table: {table}. Choices: {list(QUERY_MAP)}")
            continue
        try:
            QUERY_MAP[table](spark)
        except Exception as e:
            print(f"  FAILED {table}: {e}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", default=None)
    parser.add_argument("--all",   action="store_true")
    args = parser.parse_args()
    tables = list(QUERY_MAP) if args.all else [args.table or "patient_summary"]
    run(tables)
