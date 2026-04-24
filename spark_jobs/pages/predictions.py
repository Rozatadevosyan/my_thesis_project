import streamlit as st
import pandas as pd
import time
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from db import get_connection


# @st.cache_data
# def load_data():
#     conn = get_connection()
#
#     query = """
#     SELECT
#         glucose_level,
#         blood_pressure
#     FROM patient_visits
#     """
#
#     df = pd.read_sql(query, conn)
#     conn.close()
#
#     # ---------------------------
#     # CLEAN blood pressure safely
#     # ---------------------------
#     bp = df["blood_pressure"].astype(str).str.replace(" ", "")
#
#     bp_split = bp.str.split("/", n=1, expand=True)
#
#     df["systolic"] = pd.to_numeric(bp_split[0], errors="coerce")
#     df["diastolic"] = pd.to_numeric(bp_split[1] if bp_split.shape[1] > 1 else None, errors="coerce")
#
#     # DON'T fully destroy dataset
#     df = df.dropna(subset=["systolic", "glucose_level"])
#     df["diastolic"] = df["diastolic"].fillna(df["systolic"] / 2)
#     return df
#
#
# df = load_data()
#
# # ---------------------------
# # TARGET (risk score)
# # ---------------------------
# df["risk_score"] = (
#     (df["glucose_level"] / 200) +
#     (df["systolic"] / 180) +
#     (df["diastolic"] / 120)
# )/3
#
# # ---------------------------
# # FEATURES & SPLIT
# # ---------------------------
# X = df[["glucose_level", "systolic", "diastolic"]]
# y = df["risk_score"]
#
# X_train, X_test, y_train, y_test = train_test_split(
#     X, y, test_size=0.2, random_state=42
# )
#
# # ---------------------------
# # MODEL
# # ---------------------------
# model = LinearRegression()
#
# times = []
#
# # Train model multiple times
# for i in range(100):
#
#     start_time = time.time()
#
#     model.fit(X_train, y_train)
#
#     end_time = time.time()
#
#     times.append(end_time - start_time)
#
# # Average training time
# training_time = sum(times)
# # ---------------------------
# # UI
# # ---------------------------
# st.header("AI Prediction (Regression Model)")
#
# st.subheader("Enter Patient Data")
#
# glucose = st.slider("Glucose Level", 70, 250, 120)
# systolic = st.slider("Systolic Pressure", 90, 200, 130)
# diastolic = st.slider("Diastolic Pressure", 60, 120, 80)
#
# # ---------------------------
# # PREDICTION
# # ---------------------------
# prediction = model.predict([[glucose, systolic, diastolic]])[0]
#
# st.metric("Predicted Risk Score", round(prediction, 3))
#
# st.subheader("Model Training Times")
#
# st.write(f"Model Training Time (seconds)(Partitioned): {5.24:.2f} seconds")
# st.write(f"Model Training Time (seconds)(Non-partitioned): {3.12:.2f} seconds")

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.getOrCreate()

# -------------------------
# NON-PARTITIONED
# -------------------------

start_np = time.time()

df_np = spark.read.parquet(
    "hdfs://localhost:9000/medical_data/patient_visits"
)

# Extract systolic
df_np = df_np.withColumn(
    "systolic",
    split(col("blood_pressure"), "/")[0].cast("int")
)

# Risk score
df_np = df_np.withColumn(
    "risk_score",
    (col("glucose_level")/200 +
     col("systolic")/180)/2
)

assembler = VectorAssembler(
    inputCols=["glucose_level", "systolic"],
    outputCol="features"
)

df_np = assembler.transform(df_np)

lr = LinearRegression(
    featuresCol="features",
    labelCol="risk_score"
)

# Train multiple times
for i in range(200):
    model_np = lr.fit(df_np)

end_np = time.time()

time_np = end_np - start_np


# -------------------------
# PARTITIONED
# -------------------------

start_p = time.time()

df_p = spark.read.parquet(
    "hdfs://localhost:9000/medical_data/patient_visits_partitioned"
)

df_p = df_p.withColumn(
    "systolic",
    split(col("blood_pressure"), "/")[0].cast("int")
)

df_p = df_p.withColumn(
    "risk_score",
    (col("glucose_level")/200 +
     col("systolic")/180)/2
)

df_p = assembler.transform(df_p)

for i in range(200):
    model_p = lr.fit(df_p)

end_p = time.time()

time_p = end_p - start_p


# -------------------------
# RESULTS
# -------------------------

print("\nTraining Time Comparison")

print("Non-partitioned:", round(time_np, 3), "sec")

print("Partitioned:", round(time_p, 3), "sec")

model_p.write().overwrite().save(
    "hdfs://localhost:9000/models/risk_model_partitioned"
)


model_np.write().overwrite().save(
    "hdfs://localhost:9000/models/risk_model_non_partitioned"
)


