import streamlit as st
import pandas as pd
from sklearn.linear_model import LinearRegression
from db import get_connection

@st.cache_data
def load_data():
    conn = get_connection()

    query = """
    SELECT 
        glucose_level,
        blood_pressure,
        heart_rate,
        temperature
    FROM patient_visits
    """

    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()

# ---------------------------
# TARGET (risk score)
# ---------------------------
df["risk_score"] = (
    df["glucose_level"]/200 +
    df["blood_pressure"]/180
) / 2

# ---------------------------
# FEATURES
# ---------------------------
X = df[["glucose_level", "blood_pressure"]]
y = df["risk_score"]

# ---------------------------
# MODEL
# ---------------------------
model = LinearRegression()
model.fit(X, y)

st.header("AI Prediction (Regression Model)")

# ---------------------------
# USER INPUT
# ---------------------------
st.subheader("Enter Patient Data")

glucose = st.slider("Glucose Level", 70, 250, 120)
bp = st.slider("Blood Pressure", 90, 200, 130)

# ---------------------------
# PREDICTION
# ---------------------------
prediction = model.predict([[glucose, bp]])[0]

st.metric("Predicted Risk Score", round(prediction, 3))