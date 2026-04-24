import streamlit as st
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession

st.set_page_config(layout="wide")

# ---------------------------
# SPARK SESSION (HDFS)
# ---------------------------
spark = SparkSession.builder \
    .appName("Streamlit HDFS Analytics") \
    .master("local[*]") \
    .getOrCreate()

# ---------------------------
# LOAD FROM HDFS
# ---------------------------
@st.cache_data
def load_data():
    df = spark.read.parquet(
        "hdfs://localhost:9000/medical_data/patient_visits"
    )
    return df.toPandas()   # convert to pandas for Streamlit

df = load_data()

st.title("📊 Analytics (HDFS Data)")

# ---------------------------
# AGE GROUPS
# ---------------------------
df["age_group"] = pd.cut(
    df["age"],
    bins=[0, 18, 40, 60, 100],
    labels=["0-18", "18-40", "40-60", "60+"]
)

age_counts = df["age_group"].value_counts().sort_index()

# ---------------------------
# DIAGNOSIS
# ---------------------------
diag_counts = df["diagnosis"].value_counts()

# ---------------------------
# LAYOUT (2 columns)
# ---------------------------
col1, col2 = st.columns(2)

# ---- AGE GROUP CHART ----
with col1:
    st.subheader("Patients by Age Group")

    fig1 = px.bar(
        x=age_counts.index,
        y=age_counts.values,
        labels={"x": "Age Group", "y": "Number of Patients"},
        title="Age Distribution",
        text=age_counts.values
    )

    fig1.update_layout(
        xaxis_tickangle=0,
        template="plotly_white"
    )

    st.plotly_chart(fig1, use_container_width=True)

# ---- DIAGNOSIS CHART ----
with col2:
    st.subheader("Visits by Diagnosis")

    fig2 = px.bar(
        x=diag_counts.index,
        y=diag_counts.values,
        labels={"x": "Diagnosis", "y": "Number of Visits"},
        title="Diagnosis Distribution",
        text=diag_counts.values
    )

    fig2.update_layout(
        xaxis_tickangle=0,
        template="plotly_white"
    )

    st.plotly_chart(fig2, use_container_width=True)

# ---------------------------
# EXTRA INSIGHTS (cards)
# ---------------------------
st.markdown("### Key Insights")

col3, col4, col5 = st.columns(3)

with col3:
    st.metric("Total Records", len(df))

with col4:
    st.metric("Most Common Diagnosis", diag_counts.idxmax())

with col5:
    st.metric("Most Common Age Group", age_counts.idxmax())