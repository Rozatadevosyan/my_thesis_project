import time
import json
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HDFSWritePerformanceComparison")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

print("Spark Session Started")

source_path = "hdfs://namenode:9000/medical_data/patient_visits"
non_partitioned_write_path = "hdfs://namenode:9000/medical_data/write_test_non_partitioned"
partitioned_write_path = "hdfs://namenode:9000/medical_data/write_test_partitioned"

df = spark.read.parquet(source_path).limit(5000)

def measure_write_time_non_partitioned():
    start = time.perf_counter()
    df.write.mode("overwrite").parquet(non_partitioned_write_path)
    end = time.perf_counter()
    return round((end - start) * 1000, 2)

def measure_write_time_partitioned():
    start = time.perf_counter()
    df.write.mode("overwrite").partitionBy("diagnosis").parquet(partitioned_write_path)
    end = time.perf_counter()
    return round((end - start) * 1000, 2)

non_part_write_time = measure_write_time_non_partitioned()
part_write_time = measure_write_time_partitioned()

results = {
    "sample_size": 5000,
    "non_partitioned_write_time_ms": non_part_write_time,
    "partitioned_write_time_ms": part_write_time
}

with open("/opt/spark/jobs/write_performance_results.json", "w") as f:
    json.dump(results, f, indent=4)

print("\n=== WRITE TIME COMPARISON (ms) ===")
print(f"Non-partitioned write time: {non_part_write_time}")
print(f"Partitioned write time: {part_write_time}")
print("Results saved to /opt/spark/jobs/write_performance_results.json")

spark.stop()
print("Done")