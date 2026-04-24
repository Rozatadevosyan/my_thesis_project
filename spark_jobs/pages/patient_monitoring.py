import streamlit as st
import pandas as pd
import time

from pyspark.sql import SparkSession

# ---------------------------
# Spark session
# ---------------------------
spark = SparkSession.builder \
    .appName("Patient Monitoring") \
    .getOrCreate()


# ---------------------------
# LOAD NON-PARTITIONED
# ---------------------------
@st.cache_data
def load_non_partitioned():

    df_np = spark.read.parquet(
        "hdfs://localhost:9000/medical_data/patient_visits"
    )

    return df_np.toPandas()


# ---------------------------
# LOAD PARTITIONED
# ---------------------------
@st.cache_data
def load_partitioned():

    df_p = spark.read.parquet(
        "hdfs://localhost:9000/medical_data/patient_visits_partitioned"
    )

    return df_p.toPandas()


# ---------------------------
# Timing Non-partitioned
# ---------------------------
start_np = time.time()

df_np = load_non_partitioned()

end_np = time.time()

time_np = end_np - start_np


# ---------------------------
# Timing Partitioned
# ---------------------------
start_p = time.time()

df_p = load_partitioned()

end_p = time.time()

time_p = end_p - start_p


# ---------------------------
# UI
# ---------------------------
st.header("Patient Monitoring (HDFS)")

# Use partitioned dataset for UI
df = df_p

# Diagnosis filter
diagnoses = sorted(df["diagnosis"].unique())

selected_diagnosis = st.selectbox(
    "Select Diagnosis",
    diagnoses
)

filtered_df = df[
    df["diagnosis"] == selected_diagnosis
]

# Patient selection
patient_ids = sorted(
    filtered_df["patient_id"].unique()
)

selected_patient = st.selectbox(
    "Select Patient ID",
    patient_ids
)

patient_data = filtered_df[
    filtered_df["patient_id"] == selected_patient
]

# ---------------------------
# Metrics
# ---------------------------
st.metric(
    "Total Visits",
    len(patient_data)
)

st.dataframe(
    patient_data,
    use_container_width=True
)

# ---------------------------
# Show timing comparison
# ---------------------------
st.subheader("⏱ Data Loading Time Comparison")

st.write(
    f"Non-partitioned: {time_np:.2f} seconds"
)

st.write(
    f"Partitioned: {time_p:.2f} seconds"
)

# ---------------------------
# Charts
# ---------------------------
st.subheader("📈 Vital Signs Trend")

if "visit_date" in patient_data.columns:

    st.line_chart(
        patient_data.set_index("visit_date")[[
            "glucose_level",
            "heart_rate",
            "temperature"
        ]]
    )