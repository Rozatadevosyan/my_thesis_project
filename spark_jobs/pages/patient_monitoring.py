import streamlit as st
import pandas as pd
from db import get_connection

@st.cache_data
def load_data():
    conn = get_connection()

    query = """
    SELECT 
        p.patient_id,
        p.name,
        p.age,
        p.gender,
        v.visit_number,
        v.visit_date,
        v.blood_pressure,
        v.glucose_level,
        v.heart_rate,
        v.temperature,
        v.diagnosis
    FROM patients p
    JOIN patient_visits v
    ON p.patient_id = v.patient_id
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()

st.header("Patient Monitoring")

patient_ids = sorted(df["patient_id"].unique())
selected_patient = st.selectbox("Select Patient ID", patient_ids)

patient_data = df[df["patient_id"] == selected_patient]

st.metric("Total Visits", len(patient_data))

st.dataframe(patient_data, use_container_width=True)

st.subheader("📈 Vital Signs Trend")

st.line_chart(
    patient_data[["blood_pressure", "glucose_level", "heart_rate"]]
)