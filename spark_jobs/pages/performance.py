import streamlit as st
import pandas as pd
import plotly.express as px
import time
from pyspark.sql import SparkSession

st.header("⚡ Performance Comparison (Real Measured)")

spark = SparkSession.builder \
    .appName("Performance Test") \
    .master("local[*]") \
    .getOrCreate()

# ---------------------------
# FUNCTION: measure read time
# ---------------------------
def measure_read(path):
    start = time.time()
    df = spark.read.parquet(path)
    df.count()   # force execution
    end = time.time()
    return (end - start) * 1000  # ms

# ---------------------------
# TEST 1: NON-PARTITIONED
# ---------------------------
t1 = measure_read("hdfs://localhost:9000/medical_data/patient_visits")

# ---------------------------
# TEST 2: PARTITIONED
# ---------------------------
t2 = measure_read("hdfs://localhost:9000/medical_data/patient_visits_partitioned")

# ---------------------------
# SHOW RESULTS
# ---------------------------
data = pd.DataFrame({
    "Type": ["Non-partitioned", "Partitioned"],
    "Read Time (ms)": [t1, t2]
})

fig = px.bar(
    data,
    x="Type",
    y="Read Time (ms)",
    text="Read Time (ms)",
    title="Real HDFS Read Time Comparison"
)

st.plotly_chart(fig, use_container_width=True)