import json
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------
# LOAD JSON FILES
# ---------------------------
with open("results/performance_results.json", "r") as f:
    perf = json.load(f)

with open("results/write_performance_results.json", "r") as f:
    write_perf = json.load(f)

read_times = perf["read_times"]
diagnoses_perf = perf["diagnoses"]
non_filter_times = perf["non_filter_times"]
part_filter_times = perf["part_filter_times"]

non_write_time = write_perf["non_partitioned_write_time_ms"]
part_write_time = write_perf["partitioned_write_time_ms"]

# ---------------------------
# CONNECT TO POSTGRESQL
# ---------------------------
conn = psycopg2.connect(
    host="localhost",
    database="thesis_db",
    user="postgres",
    password="postgres123",
    port="5440"
)

# ---------------------------
# PATIENTS BY AGE GROUP
# ---------------------------
age_query = """
SELECT
    CASE
        WHEN age < 18 THEN '0-18'
        WHEN age >= 18 AND age < 40 THEN '18-40'
        WHEN age >= 40 AND age < 60 THEN '40-60'
        ELSE '60+'
    END AS age_group,
    COUNT(*) AS patient_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM patients
GROUP BY 1
ORDER BY 1;
"""
age_df = pd.read_sql(age_query, conn)

# ---------------------------
# VISITS BY DIAGNOSIS
# ---------------------------
visits_query = """
SELECT
    diagnosis,
    COUNT(*) AS visit_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM patient_visits
GROUP BY diagnosis
ORDER BY visit_count DESC;
"""
visits_df = pd.read_sql(visits_query, conn)

# ---------------------------
# DATASET OVERVIEW
# ---------------------------
overview_query = """
SELECT 'patients' AS table_name, COUNT(*) AS record_count FROM patients
UNION ALL
SELECT 'patient_visits', COUNT(*) FROM patient_visits
UNION ALL
SELECT 'lab_results', COUNT(*) FROM lab_results
UNION ALL
SELECT 'treatments', COUNT(*) FROM treatments;
"""
overview_df = pd.read_sql(overview_query, conn)

conn.close()

# ---------------------------
# FILTER PERFORMANCE TABLE
# ---------------------------
filter_table_df = pd.DataFrame({
    "Diagnosis": diagnoses_perf,
    "Non-partitioned (ms)": non_filter_times,
    "Partitioned (ms)": part_filter_times
})
filter_table_df["Improvement (ms)"] = (
    filter_table_df["Non-partitioned (ms)"] - filter_table_df["Partitioned (ms)"]
).round(2)
filter_table_df["Improvement (%)"] = (
    filter_table_df["Improvement (ms)"] / filter_table_df["Non-partitioned (ms)"] * 100
).round(2)

# ---------------------------
# READ PERFORMANCE TABLE
# ---------------------------
read_table_df = pd.DataFrame({
    "Run Type": [
        "Non-partitioned 1st Run",
        "Partitioned 1st Run",
        "Non-partitioned 2nd Run",
        "Partitioned 2nd Run"
    ],
    "Time (ms)": read_times
})

# ---------------------------
# WRITE PERFORMANCE TABLE
# ---------------------------
write_table_df = pd.DataFrame({
    "Storage Type": ["Non-partitioned", "Partitioned"],
    "Write Time (ms)": [non_write_time, part_write_time]
})

# ---------------------------
# SAVE TABLES AS CSV
# ---------------------------
overview_df.to_csv("table_dataset_overview.csv", index=False)
age_df.to_csv("table_patients_by_age_group.csv", index=False)
visits_df.to_csv("table_visits_by_diagnosis.csv", index=False)
read_table_df.to_csv("table_read_time_comparison.csv", index=False)
filter_table_df.to_csv("table_filter_time_by_diagnosis.csv", index=False)
write_table_df.to_csv("table_write_time_comparison.csv", index=False)

# ---------------------------
# GRAPH 1: READ TIME COMPARISON
# ---------------------------
read_categories = [
    "Non-partitioned\n1st Run",
    "Partitioned\n1st Run",
    "Non-partitioned\n2nd Run",
    "Partitioned\n2nd Run"
]

plt.figure(figsize=(10, 6))
plt.bar(read_categories, read_times)
plt.title("Comparison of Read Time for Partitioned and Non-Partitioned Datasets")
plt.ylabel("Time (ms)")

for i, v in enumerate(read_times):
    plt.text(i, v + 15, f"{v:.2f}", ha="center", fontsize=9)

plt.tight_layout()
plt.savefig("graph_read_time_comparison.png", dpi=300)
plt.show()

# ---------------------------
# GRAPH 2: FILTER TIME BY DIAGNOSIS
# ---------------------------
x = range(len(diagnoses_perf))
width = 0.35

plt.figure(figsize=(10, 6))
plt.bar([i - width/2 for i in x], non_filter_times, width=width, label="Non-partitioned")
plt.bar([i + width/2 for i in x], part_filter_times, width=width, label="Partitioned")

plt.title("Filter Time by Diagnosis: Non-partitioned vs Partitioned")
plt.ylabel("Time (ms)")
plt.xticks(list(x), diagnoses_perf, rotation=0)
plt.legend()

for i, v in enumerate(non_filter_times):
    plt.text(i - width/2, v + 5, f"{v:.2f}", ha="center", fontsize=8)

for i, v in enumerate(part_filter_times):
    plt.text(i + width/2, v + 5, f"{v:.2f}", ha="center", fontsize=8)

plt.tight_layout()
plt.savefig("graph_filter_time_by_diagnosis.png", dpi=300)
plt.show()

# ---------------------------
# GRAPH 3: PATIENTS BY AGE GROUP
# ---------------------------
plt.figure(figsize=(8, 6))
plt.bar(age_df["age_group"], age_df["patient_count"])
plt.title("Patients by Age Group")
plt.ylabel("Number of Patients")

for i, v in enumerate(age_df["patient_count"]):
    plt.text(i, v + 20, str(v), ha="center", fontsize=9)

plt.tight_layout()
plt.savefig("graph_patients_by_age_group.png", dpi=300)
plt.show()

# ---------------------------
# GRAPH 4: VISITS BY DIAGNOSIS
# ---------------------------
plt.figure(figsize=(9, 6))
plt.bar(visits_df["diagnosis"], visits_df["visit_count"])
plt.title("Visits by Diagnosis")
plt.ylabel("Number of Visits")
plt.xticks(rotation=0)

for i, v in enumerate(visits_df["visit_count"]):
    plt.text(i, v + 20, str(v), ha="center", fontsize=9)

plt.tight_layout()
plt.savefig("graph_visits_by_diagnosis.png", dpi=300)
plt.show()

# ---------------------------
# GRAPH 5: WRITE TIME COMPARISON
# ---------------------------
plt.figure(figsize=(8, 6))
write_labels = ["Non-partitioned", "Partitioned"]
write_values = [non_write_time, part_write_time]

plt.bar(write_labels, write_values)
plt.title("Write Time Comparison")
plt.ylabel("Time (ms)")

for i, v in enumerate(write_values):
    plt.text(i, v + 10, f"{v:.2f}", ha="center", fontsize=9)

plt.tight_layout()
plt.savefig("graph_write_time_comparison.png", dpi=300)
plt.show()

print("All graphs generated successfully.")
print("All tables saved as CSV.")