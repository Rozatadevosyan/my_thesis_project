import time
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("HDFSPerformanceComparison")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .config("spark.hadoop.dfs.datanode.use.datanode.hostname", "true")
    .getOrCreate()
)

print("Spark Session Started")

non_partitioned_path = "hdfs://namenode:9000/medical_data/patient_visits"
partitioned_path = "hdfs://namenode:9000/medical_data/patient_visits_partitioned"

def measure_read_time(path):
    start = time.perf_counter()
    df = spark.read.parquet(path)
    df.count()
    end = time.perf_counter()
    return round((end - start) * 1000, 2)

def measure_filter_time(path, diagnosis_value):
    start = time.perf_counter()
    df = spark.read.parquet(path)
    df.filter(col("diagnosis") == diagnosis_value).count()
    end = time.perf_counter()
    return round((end - start) * 1000, 2)

print("Measuring read times...")
non_read_1 = measure_read_time(non_partitioned_path)
part_read_1 = measure_read_time(partitioned_path)
non_read_2 = measure_read_time(non_partitioned_path)
part_read_2 = measure_read_time(partitioned_path)

diagnoses = ["Diabetes", "Hypertension", "Infection", "General Checkup"]

print("Measuring filter times...")
non_filter_times = []
part_filter_times = []

for d in diagnoses:
    non_filter_times.append(measure_filter_time(non_partitioned_path, d))
    part_filter_times.append(measure_filter_time(partitioned_path, d))

results = {
    "read_categories": [
        "Non-partitioned 1st Run",
        "Partitioned 1st Run",
        "Non-partitioned 2nd Run",
        "Partitioned 2nd Run"
    ],
    "read_times": [non_read_1, part_read_1, non_read_2, part_read_2],
    "diagnoses": diagnoses,
    "non_filter_times": non_filter_times,
    "part_filter_times": part_filter_times
}

with open("/opt/spark/jobs/performance_results.json", "w") as f:
    json.dump(results, f, indent=4)

print("\n=== READ TIME COMPARISON (ms) ===")
print(f"Non-partitioned 1st run: {non_read_1}")
print(f"Partitioned 1st run: {part_read_1}")
print(f"Non-partitioned 2nd run: {non_read_2}")
print(f"Partitioned 2nd run: {part_read_2}")

print("\n=== FILTER TIME BY DIAGNOSIS (ms) ===")
for i, d in enumerate(diagnoses):
    print(f"{d}: non-partitioned={non_filter_times[i]}, partitioned={part_filter_times[i]}")

print("\nResults saved to /opt/spark/jobs/performance_results.json")

spark.stop()
print("Done")