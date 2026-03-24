"""
utils/minio_setup.py
Bootstrap MinIO bucket structure and verify S3A connectivity.
Run once before any Spark jobs:  python utils/minio_setup.py --verify-spark
"""

import os, argparse, sys, io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    print("Run:  pip install minio")
    sys.exit(1)

MINIO_HOST       = os.getenv("MINIO_HOST",       "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET           = os.getenv("MINIO_BUCKET",     "spark-data")

PREFIXES = [
    "ehr/bronze/patients/",
    "ehr/bronze/adt_events/",
    "ehr/bronze/lab_results/",
    "ehr/bronze/vitals/",
    "ehr/silver/patients/",
    "ehr/silver/adt_events/",
    "ehr/silver/lab_results/",
    "ehr/silver/vitals/",
    "ehr/gold/patient_summary/",
    "ehr/gold/encounter_metrics/",
    "ehr/gold/daily_census/",
    "ehr/gold/critical_alerts/",
    "ehr/gold/quality_dashboard/",
]


def setup_minio() -> bool:
    client = Minio(
        MINIO_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    try:
        if not client.bucket_exists(BUCKET):
            client.make_bucket(BUCKET)
            print(f"Created bucket: {BUCKET}")
        else:
            print(f"Bucket already exists: {BUCKET}")
    except S3Error as e:
        print(f"Cannot connect to MinIO at {MINIO_HOST}: {e}")
        print("Is MinIO running?  docker ps | grep minio")
        return False

    for prefix in PREFIXES:
        try:
            client.put_object(BUCKET, f"{prefix}.keep", io.BytesIO(b""), length=0)
        except S3Error:
            pass

    print(f"{len(PREFIXES)} folder prefixes ready in s3://{BUCKET}/")

    print(f"Top-level folders in {BUCKET}:")
    seen = set()
    for obj in client.list_objects(BUCKET, recursive=False):
        top = obj.object_name.split("/")[0]
        if top not in seen:
            print(f"  {top}/")
            seen.add(top)

    return True


def verify_spark_s3a():
    from utils.spark_session import get_spark, MINIO_BUCKET
    spark = get_spark("S3A_Connectivity_Test", enable_minio=True)
    test_path = f"s3a://{MINIO_BUCKET}/ehr/_spark_test"
    print(f"Spark S3A write test -> {test_path}")
    try:
        df = spark.createDataFrame([("ok", 1)], ["status", "val"])
        df.write.format("parquet").mode("overwrite").save(test_path)
        print("write OK")
        df2 = spark.read.parquet(test_path)
        df2.show()
        print("read OK")
        print("S3A <-> MinIO confirmed - jar conflict resolved!")
    except Exception as e:
        print(f"S3A test failed: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify-spark", action="store_true")
    args = parser.parse_args()
    ok = setup_minio()
    if ok and args.verify_spark:
        verify_spark_s3a()
