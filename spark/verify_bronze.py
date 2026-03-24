from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("verify") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/tmp/ehr/bronze/patients/")
print(f"\nTotal patients in Bronze: {df.count()}")
df.select("patient_id", "mrn_hash", "age_group", "gender", "pipeline_ts").show(10, truncate=False)
spark.stop()
