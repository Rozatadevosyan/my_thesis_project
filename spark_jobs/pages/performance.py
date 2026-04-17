import streamlit as st
import pandas as pd
import plotly.express as px

st.header("⚡ Performance Comparison")

# ---------------------------
# READ TIME
# ---------------------------
data = {
    "Type": [
        "Non-partitioned Read",
        "Partitioned Read",
        "Non-partitioned 2nd",
        "Partitioned 2nd"
    ],
    "Small Dataset": [2235, 212, 156, 166],
    "Large Dataset": [143, 152, 151, 148]
}

df = pd.DataFrame(data)

df_melt = df.melt(id_vars="Type", var_name="Dataset", value_name="Time")

fig1 = px.bar(
    df_melt,
    x="Type",
    y="Time",
    color="Dataset",
    barmode="group",
    text="Time",
    title="Read Time Comparison (ms)"
)

st.plotly_chart(fig1, use_container_width=True)

# ---------------------------
# FILTER TIME
# ---------------------------
filter_data = {
    "Diagnosis": ["Diabetes", "Hypertension", "Infection", "General Checkup"],
    "Non-partitioned": [315, 159, 147, 128],
    "Partitioned": [167, 155, 130, 117]
}

df2 = pd.DataFrame(filter_data)

df2_melt = df2.melt(id_vars="Diagnosis", var_name="Type", value_name="Time")

fig2 = px.bar(
    df2_melt,
    x="Diagnosis",
    y="Time",
    color="Type",
    barmode="group",
    text="Time",
    title="Filter Time by Diagnosis (ms)"
)

st.plotly_chart(fig2, use_container_width=True)