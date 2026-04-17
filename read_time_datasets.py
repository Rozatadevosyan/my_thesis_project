import json
import matplotlib.pyplot as plt

with open("spark_jobs/performance_results.json", "r") as f:
    data = json.load(f)

read_categories = [
    "Non-partitioned\n1st Run",
    "Partitioned\n1st Run",
    "Non-partitioned\n2nd Run",
    "Partitioned\n2nd Run"
]
read_times = data["read_times"]

diagnoses = data["diagnoses"]
non_filter_times = data["non_filter_times"]
part_filter_times = data["part_filter_times"]

# Graph 1
plt.figure(figsize=(10, 6))
plt.bar(read_categories, read_times)
plt.title("Comparison of Read Time for Partitioned and Non-Partitioned Datasets")
plt.ylabel("Time (ms)")

for i, v in enumerate(read_times):
    plt.text(i, v + 10, f"{v:.2f}", ha="center", fontsize=9)

plt.tight_layout()
plt.savefig("read_time_comparison.png", dpi=300)
plt.show()

# Graph 2
x = range(len(diagnoses))
width = 0.35

plt.figure(figsize=(10, 6))
plt.bar([i - width/2 for i in x], non_filter_times, width=width, label="Non-partitioned")
plt.bar([i + width/2 for i in x], part_filter_times, width=width, label="Partitioned")

plt.title("Diagnosis-Based Filter Performance")
plt.ylabel("Time (ms)")
plt.xticks(list(x), diagnoses, rotation=0)
plt.legend()

for i, v in enumerate(non_filter_times):
    plt.text(i - width/2, v + 5, f"{v:.2f}", ha="center", fontsize=8)

for i, v in enumerate(part_filter_times):
    plt.text(i + width/2, v + 5, f"{v:.2f}", ha="center", fontsize=8)

plt.tight_layout()
plt.savefig("filter_time_comparison.png", dpi=300)
plt.show()