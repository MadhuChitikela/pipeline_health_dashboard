
import streamlit as st
try:
    from snowflake.snowpark.context import get_active_session
except ImportError:
    # Fallback for local testing if snowflake-snowpark-python is not fully configured or used outside Snowflake
    def get_active_session():
        # This is a placeholder. In a real scenario, you would create a session manually if needed.
        # Returning None to handle potential errors gracefully
        return None

import sys
import os

# Ensure we can import modules if running from root or nested
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.data_loader import *
from processing.transformations import *
from components.charts import *
from utils.scoring import *

st.set_page_config(page_title="Pipeline Health Dashboard", layout="wide")

st.title("Pipeline Health Dashboard")

session = None
try:
    session = get_active_session()
except Exception:
    session = None

# --- Load Data (With Fallback for View Mode) ---
try:
    # If session is None, loaders return empty DataFrames (Schema Validation Mode)
    df_jobs = load_job_timeliness(session)
    df_sources = load_sources(session)
    df_outputs = load_outputs(session)
    df_uniqueness = load_uniqueness(session)
    df_integrity = load_integrity(session)

except Exception as e:
    # This block handles unexpected exceptions within loaders, though loaders catch most errors internally now
    st.error("Data loading requires execution inside Snowflake Streamlit environment or valid local configuration.")
    st.stop()

if session is None:
    st.info("ℹ️ Running in View Mode (No Snowflake Connection). Showing layout with empty data.")

# --- Transformations ---
# Jobs
df_jobs = standardize_datetimes(df_jobs, ["PIPELINE_START_TIME", "JOB_START_TIME", "END_TIME"])
df_jobs = calculate_duration_minutes(df_jobs)
df_jobs = detect_sla_breach(df_jobs)
df_jobs = add_week_period(df_jobs, "PIPELINE_START_TIME")
df_jobs = map_execution_status(df_jobs)

# Other Data Frames (Standardize if needed for future features)
df_sources = standardize_datetimes(df_sources, ["PIPELINE_START_TIME"])
df_uniqueness = standardize_datetimes(df_uniqueness, ["PIPELINE_START_TIME"])
df_integrity = standardize_datetimes(df_integrity, ["PIPELINE_START_TIME"])

# --- KPIS ---
if not df_jobs.empty:
    total_pipelines = df_jobs["PIPELINE_NAME"].nunique()
    failures = df_jobs[df_jobs["STATUS"] == "FAIL"]["PIPELINE_NAME"].nunique()
else:
    total_pipelines = 0
    failures = 0

col1, col2 = st.columns(2)
col1.metric("Pipelines Monitored", total_pipelines)
col2.metric("Pipelines Failing", failures)

st.divider()

# --- Charts ---
st.plotly_chart(duration_trend_chart(df_jobs), use_container_width=True)
st.plotly_chart(sla_breach_chart(df_jobs), use_container_width=True)
