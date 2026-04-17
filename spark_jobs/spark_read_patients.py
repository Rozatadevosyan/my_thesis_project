import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

spark = (
    SparkSession.builder
    .appName("PostgreSQL to HDFS Medical Pipeline")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

print("✅ Spark Session Started")

# ---------------------------
# Read from PostgreSQL
# ---------------------------
patients = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/thesis_db")
    .option("dbtable", "patients")
    .option("user", "postgres")
    .option("password", "postgres123")
    .option("driver", "org.postgresql.Driver")
    .load()
)

visits = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/thesis_db")
    .option("dbtable", "patient_visits")
    .option("user", "postgres")
    .option("password", "postgres123")
    .option("driver", "org.postgresql.Driver")
    .load()
)

labs = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/thesis_db")
    .option("dbtable", "lab_results")
    .option("user", "postgres")
    .option("password", "postgres123")
    .option("driver", "org.postgresql.Driver")
    .load()
)

treatments = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/thesis_db")
    .option("dbtable", "treatments")
    .option("user", "postgres")
    .option("password", "postgres123")
    .option("driver", "org.postgresql.Driver")
    .load()
)

print("✅ Data loaded from PostgreSQL")
print("Patients count:", patients.count())
print("Visits count:", visits.count())
print("Lab results count:", labs.count())
print("Treatments count:", treatments.count())

# ---------------------------
# Analytics
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
# Save raw tables to HDFS
# ---------------------------
patients.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/patients")

visits.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/patient_visits")

labs.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/lab_results")

treatments.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/treatments")

# ---------------------------
# Save analytics to HDFS
# ---------------------------
patients_by_gender.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/patients_by_gender")

patients_by_age_group.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/patients_by_age_group")

visits_by_diagnosis.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/visits_by_diagnosis")

visits_by_department.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/visits_by_department")

avg_vitals_by_diagnosis.write \
    .mode("overwrite") \
    .option("dfs.replication", "1") \
    .parquet("hdfs://namenode:9000/medical_data/avg_vitals_by_diagnosis")

print("✅ Raw and analytics datasets saved to HDFS")

spark.stop()
print("🎯 Pipeline completed successfully")