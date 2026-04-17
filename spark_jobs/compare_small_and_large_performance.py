import time
import json
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Compare Small And Large Read Performance")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

print("Spark Session Started")

small_non_partitioned_path = "hdfs://namenode:9000/medical_data/patient_visits_small"
small_partitioned_path = "hdfs://namenode:9000/medical_data/patient_visits_small_partitioned"

large_non_partitioned_path = "hdfs://namenode:9000/medical_data/patient_visits"
large_partitioned_path = "hdfs://namenode:9000/medical_data/patient_visits_partitioned"

def measure_read_time(path):
    start = time.perf_counter()
    df = spark.read.parquet(path)
    df.count()
    end = time.perf_counter()
    return round((end - start) * 1000, 2)

small_non_read_1 = measure_read_time(small_non_partitioned_path)
small_part_read_1 = measure_read_time(small_partitioned_path)
small_non_read_2 = measure_read_time(small_non_partitioned_path)
small_part_read_2 = measure_read_time(small_partitioned_path)

large_non_read_1 = measure_read_time(large_non_partitioned_path)
large_part_read_1 = measure_read_time(large_partitioned_path)
large_non_read_2 = measure_read_time(large_non_partitioned_path)
large_part_read_2 = measure_read_time(large_partitioned_path)

results = {
    "categories": [
        "Non-partitioned Read",
        "Partitioned Read",
        "Non-partitioned 2nd Run",
        "Partitioned 2nd Run"
    ],
    "small_times": [
        small_non_read_1,
        small_part_read_1,
        small_non_read_2,
        small_part_read_2
    ],
    "large_times": [
        large_non_read_1,
        large_part_read_1,
        large_non_read_2,
        large_part_read_2
    ]
}

with open("/opt/spark/jobs/small_large_read_results.json", "w") as f:
    json.dump(results, f, indent=4)

print("\n=== SMALL DATASET (ms) ===")
print(f"Non-partitioned 1st run: {small_non_read_1}")
print(f"Partitioned 1st run: {small_part_read_1}")
print(f"Non-partitioned 2nd run: {small_non_read_2}")
print(f"Partitioned 2nd run: {small_part_read_2}")

print("\n=== LARGE DATASET (ms) ===")
print(f"Non-partitioned 1st run: {large_non_read_1}")
print(f"Partitioned 1st run: {large_part_read_1}")
print(f"Non-partitioned 2nd run: {large_non_read_2}")
print(f"Partitioned 2nd run: {large_part_read_2}")

spark.stop()
print("Done")