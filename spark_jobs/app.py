import streamlit as st

st.set_page_config(page_title="Hospital Monitoring System", layout="wide", )

st.title("🏥Hospital Monitoring System")

st.sidebar.title("Navigation")

page = st.sidebar.radio("Go to", [
    "Patient Monitoring",
    "Analytics",
    "Performance",
    "Predictions"
])

if page == "Patient Monitoring":
    import pages.patient_monitoring

elif page == "Analytics":
    import pages.analytics

elif page == "Performance":
    import pages.performance

elif page == "Predictions":
    import pages.predictions