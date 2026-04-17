import streamlit as st
import pandas as pd
import psycopg2
import time
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from pyspark.sql import SparkSession
st.set_page_config(page_title="Hospital Monitoring System", layout="wide")

st.title("🏥Hospital Monitoring System")

# ---------------------------
# DATABASE CONNECTION
# ---------------------------

conn = psycopg2.connect(
    host="localhost",
    database="thesis_db",
    user="postgres",
    password="postgres123",
    port="5440"
)
@st.cache_resource
def get_spark():
    return SparkSession.builder \
    .appName("HospitalHDFSWriter") \
    .config("spark.hadoop.fs.defaultFS","hdfs://localhost:9000") \
    .getOrCreate()

spark = get_spark()

# ---------------------------
# LOAD DATA
# ---------------------------

query = """
SELECT p.patient_id,
       p.name,
       v.visit_number,
       v.visit_date,
       v.blood_pressure,
       v.glucose_level,
       v.heart_rate,
       v.temperature,
       v.disease_severity,
       v.recovery_days
FROM patients p
JOIN patient_visits v
ON p.patient_id = v.patient_id
"""

df = pd.read_sql(query, conn)

# ---------------------------
# AI MODEL (Severity)
# ---------------------------

features = [
    "blood_pressure",
    "glucose_level",
    "heart_rate",
    "temperature"
]

X = df[features]
y = df["disease_severity"]

model = LinearRegression()
model.fit(X, y)

df["prediction"] = model.predict(X)
# ---------------------------
# AI MODEL (Recovery Prediction)
# ---------------------------

recovery_features = [
    "blood_pressure",
    "glucose_level",
    "heart_rate",
    "temperature",
    "disease_severity"
]

X_rec = df[recovery_features]
y_rec = df["recovery_days"]

recovery_model = LinearRegression()
recovery_model.fit(X_rec, y_rec)

# ---------------------------
# PAGE NAVIGATION
# ---------------------------

page = st.sidebar.selectbox(
    "Navigation",
    ["Dashboard", "Add Visit"]
)

# ===========================
# DASHBOARD PAGE
# ===========================

if page == "Dashboard":

    st.subheader("Patient Monitoring")

    patient_ids = df["patient_id"].unique()

    selected_patient = st.selectbox(
        "Select Patient ID",
        patient_ids
    )

    # store patient in session
    st.session_state["selected_patient"] = selected_patient

    patient_data = df[df["patient_id"] == selected_patient]

    st.subheader("Patient Visits")

    # այստեղ index-ը թաքցնում ենք
    st.dataframe(patient_data, hide_index=True)

    # GRAPH
    st.subheader("Severity Progress")

    plot_start = time.perf_counter()

    fig, ax = plt.subplots()

    x = patient_data["visit_number"].astype(int)

    ax.plot(
        x,
        patient_data["disease_severity"],
        marker="o",
        label="Actual Severity"
    )

    ax.plot(
        x,
        patient_data["prediction"],
        marker="x",
        linestyle="--",
        label="AI Prediction"
    )

    ax.set_xticks(sorted(x.unique()))
    ax.set_xlabel("Visit Number")
    ax.set_ylabel("Severity Level")
    ax.legend()

    plot_end = time.perf_counter()
    plot_time = plot_end - plot_start

    st.pyplot(fig)
    st.info(f"Chart generation time: {plot_time:.6f} seconds")

    # ---------------------------
    # GET LATEST VISIT
    # ---------------------------

    latest = patient_data.sort_values("visit_number").iloc[-1]

    if pd.notna(latest["visit_date"]):
        visit_date = pd.to_datetime(latest["visit_date"])
        severity = latest["disease_severity"]

        if severity > 0.8:
            days = 1
        elif severity > 0.5:
            days = 3
        else:
            days = 7

        next_visit = visit_date + pd.Timedelta(days=days)

        st.subheader("Suggested Next Visit")
        st.write(f"Recommended next visit date: **{next_visit.date()}**")
    else:
        st.subheader("Suggested Next Visit")
        st.warning("Visit date is missing for the latest record.")
# ===========================
# ADD VISIT PAGE
# ===========================

if page == "Add Visit":

    st.subheader("Add New Patient Visit")

    # check if patient selected
    if "selected_patient" not in st.session_state:

        st.warning("⚠ First select a patient in Dashboard")
        st.stop()

    selected_patient = st.session_state["selected_patient"]

    load_start = time.perf_counter()

    patient_data = df[df["patient_id"] == selected_patient].copy()

    load_end = time.perf_counter()
    load_time = load_end - load_start

    # automatic visit number
    visit_number = int(patient_data["visit_number"].max()) + 1

    st.write(f"Patient ID: **{selected_patient}**")
    st.write(f"Visit Number: **{visit_number}**")

    # INPUTS
    visit_date = st.date_input("Visit Date")

    blood_pressure = st.number_input("Blood Pressure", min_value=0.0)

    glucose_level = st.number_input("Glucose Level", min_value=0.0)

    heart_rate = st.number_input("Heart Rate", min_value=0.0)

    temperature = st.number_input("Temperature", min_value=0.0)

    disease_severity = st.slider("Disease Severity", 0.0, 1.0)

    recovery_days = st.number_input("Recovery Days", min_value=0)


    # SAVE BUTTON

    if st.button("Add Visit"):
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO patient_visits
            (patient_id, visit_number, visit_date,
             blood_pressure, glucose_level, heart_rate,
             temperature, disease_severity, recovery_days)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                int(selected_patient),
                int(visit_number),
                visit_date,
                float(blood_pressure),
                float(glucose_level),
                float(heart_rate),
                float(temperature),
                float(disease_severity),
                int(recovery_days)
            )
        )

        conn.commit()

        # HDFS write
        hdfs_path = "hdfs://localhost:9000/medical_data/ml_dataset"

        new_visit = pd.DataFrame([{
            "patient_id": int(selected_patient),
            "visit_number": int(visit_number),
            "blood_pressure": float(blood_pressure),
            "glucose_level": float(glucose_level),
            "heart_rate": float(heart_rate),
            "temperature": float(temperature),
            "disease_severity": float(disease_severity),
            "recovery_days": int(recovery_days)
        }])

        spark_df = spark.createDataFrame(new_visit)

        try:
            existing_df = spark.read.parquet(hdfs_path)

            duplicate = existing_df.filter(
                (existing_df.patient_id == int(selected_patient)) &
                (existing_df.visit_number == int(visit_number))
            ).count()

            if duplicate == 0:

                spark_df.write \
                    .mode("append") \
                    .parquet(hdfs_path)

            else:
                st.warning("Visit already exists in HDFS")

        except:
            # first write if dataset doesn't exist
            spark_df.write \
                .mode("append") \
                .parquet(hdfs_path)
        st.success("✅ Visit added successfully")
    # SHOW PREVIOUS VISITS

    st.subheader("Previous Visits")

    # այստեղ էլ index-ը թաքցնում ենք
    st.dataframe(patient_data, hide_index=True)
    st.info(f"Patient data loading time: {load_time:.6f} seconds")

conn.close()