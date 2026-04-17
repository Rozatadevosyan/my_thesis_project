import os
os.environ["HADOOP_USER_NAME"] = "spark"

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Load Small Dataset To HDFS")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .config("spark.jars", "/opt/spark/jobs/postgresql.jar")
    .getOrCreate()
)

print("Spark Session Started")

jdbc_url = "jdbc:postgresql://postgres:5432/thesis_db"
jdbc_props = {
    "user": "postgres",
    "password": "postgres123",
    "driver": "org.postgresql.Driver"
}

# -------------------------
# Read small tables
# -------------------------
patients_small = spark.read.jdbc(
    url=jdbc_url,
    table="patients_small",
    properties=jdbc_props
)

patient_visits_small = spark.read.jdbc(
    url=jdbc_url,
    table="patient_visits_small",
    properties=jdbc_props
)

lab_results_small = spark.read.jdbc(
    url=jdbc_url,
    table="lab_results_small",
    properties=jdbc_props
)

treatments_small = spark.read.jdbc(
    url=jdbc_url,
    table="treatments_small",
    properties=jdbc_props
)

print("Small tables loaded from PostgreSQL")

# -------------------------
# Save non-partitioned
# -------------------------
patients_small.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/medical_data/patients_small"
)

patient_visits_small.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/medical_data/patient_visits_small"
)

lab_results_small.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/medical_data/lab_results_small"
)

treatments_small.write.mode("overwrite").parquet(
    "hdfs://namenode:9000/medical_data/treatments_small"
)

print("Non-partitioned small datasets saved")

# -------------------------
# Save partitioned version
# -------------------------
patient_visits_small.write \
    .mode("overwrite") \
    .partitionBy("diagnosis") \
    .parquet("hdfs://namenode:9000/medical_data/patient_visits_small_partitioned")

print("Partitioned small dataset saved")

spark.stop()
print("Done")