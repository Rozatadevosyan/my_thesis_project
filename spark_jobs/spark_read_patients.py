import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

# ---------------------------
# Spark Session
# ---------------------------
spark = SparkSession.builder \
    .appName("PostgreSQL to HDFS Medical Pipeline") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

print("✅ Spark Session Started")

# ---------------------------
# JDBC CONFIG
# ---------------------------
jdbc_url = "jdbc:postgresql://localhost:5440/thesis_db"

def read_table(table_name):
    return spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres123") \
        .option("driver", "org.postgresql.Driver") \
        .load()

# ---------------------------
# READ TABLES (FIXED)
# ---------------------------
patients = read_table("patients")
visits = read_table("patient_visits")
labs = read_table("lab_results")
treatments = read_table("treatments")

print("✅ Data loaded from PostgreSQL")

patients.printSchema()
visits.printSchema()

print("Patients count:", patients.count())
print("Visits count:", visits.count())
print("Lab results count:", labs.count())
print("Treatments count:", treatments.count())

# ---------------------------
# ANALYTICS
# ---------------------------

patients_by_gender = patients.groupBy("gender").count()

patients_by_age_group = patients.withColumn(
    "age_group",
    when(col("age") < 18, "0-18")
    .when((col("age") >= 18) & (col("age") < 40), "18-40")
    .when((col("age") >= 40) & (col("age") < 60), "40-60")
    .otherwise("60+")
).groupBy("age_group").count()

visits_by_diagnosis = visits.groupBy("diagnosis").count()

visits_by_department = visits.groupBy("department").count()

avg_vitals_by_diagnosis = visits.groupBy("diagnosis").avg(
    "blood_pressure",
    "glucose_level",
    "heart_rate",
    "temperature"
)

# ---------------------------
# SAVE TO HDFS
# ---------------------------

patients.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/patients"
)

visits.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/patient_visits"
)

labs.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/lab_results"
)

treatments.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/treatments"
)

patients_by_gender.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/patients_by_gender"
)

patients_by_age_group.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/patients_by_age_group"
)

visits_by_diagnosis.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/visits_by_diagnosis"
)

visits_by_department.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/visits_by_department"
)

avg_vitals_by_diagnosis.write.mode("overwrite").parquet(
    "hdfs://localhost:9000/medical_data/avg_vitals_by_diagnosis"
)

print("✅ All datasets saved to HDFS")

spark.stop()
print("🎯 Pipeline completed successfully")