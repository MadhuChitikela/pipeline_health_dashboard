
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

st.set_page_config(
    page_title="Pipeline Operations Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---- THEME TOGGLE ----
with st.sidebar:
    st.subheader("⚙️ Settings")
    theme_choice = st.radio("Theme Mode", ["Dark", "Light"], horizontal=True)

# ---- Custom CSS ----
if theme_choice == "Dark":
    bg_color = "#0e1117"
    text_color = "white"
    card_bg = "#161b22"
    border_color = "#30363d"
    title_color = "#4ea8de"
else: # Light Mode
    bg_color = "#ffffff"
    text_color = "#000000"
    card_bg = "#f0f2f6"
    border_color = "#e5e7eb"
    title_color = "#0369a1"

st.markdown(f"""
<style>

/* Page background */
.stApp {{
    background-color: {bg_color};
    color: {text_color};
}}

/* Section headers */
.section-title {{
    font-size: 26px;
    font-weight: 600;
    color: {title_color};
    margin-bottom: 10px;
}}

/* KPI Card */
.kpi-card {{
    background-color: {card_bg};
    padding: 25px;
    border-radius: 12px;
    box-shadow: 0px 4px 15px rgba(0,0,0,0.1);
    border-left: 6px solid #4ea8de;
    margin-bottom: 10px;
    border: 1px solid {border_color};
}}

.kpi-value {{
    font-size: 40px;
    font-weight: bold;
    color: {text_color};
}}

.kpi-title {{
    font-size: 14px;
    text-transform: uppercase;
    color: #9ca3af;
    font-weight: 600;
}}

/* Buttons */
.stButton>button {{
    border-radius: 25px;
    height: 45px;
    font-weight: 600;
    background: linear-gradient(90deg,#4ea8de,#3a86ff);
    color: white;
    border: none;
    width: 100%;
}}

.stButton>button:hover {{
    transform: scale(1.02);
    box-shadow: 0px 5px 15px rgba(58, 134, 255, 0.4);
}}

/* Table styling */
[data-testid="stDataFrame"] {{
    background-color: {card_bg};
    border-radius: 10px;
    padding: 10px;
    border: 1px solid {border_color};
}}

</style>
""", unsafe_allow_html=True)

st.title("Pipeline Health Dashboard")

session = None
try:
    session = get_active_session()
except Exception:
    session = None

# --- Load Data (With Fallback for View Mode) ---
from datetime import datetime, timedelta
import pandas as pd

# --- Load Data (Production or Simulation) ---
try:
    df_jobs = load_job_timeliness(session)
    df_sources = load_sources(session)
    df_outputs = load_outputs(session)
    df_uniqueness = load_uniqueness(session)
    df_integrity = load_integrity(session)

except Exception as e:
    st.error(f"Critical Error loading data: {e}")
    st.stop()

if session is None:
    st.info("ℹ️ Running in Local Simulation Mode (SQLite). Logic validated against production schema.")

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


# --- UI HEADER ---
st.markdown('<div class="section-title">Pipeline Operations Dashboard</div>', unsafe_allow_html=True)
st.caption("Monitor Execution Health • Detect Failures • Track Data Quality")
st.markdown("---")


# --- HELPER COMPONENT ---
def kpi_card(title, value, color):
    return f"""
    <div class="kpi-card" style="border-left:6px solid {color};">
        <div class="kpi-title" style="color:{color};">{title}</div>
        <div class="kpi-value">{value}</div>
    </div>
    """

# --- KPIS ---
if not df_jobs.empty:
    total_pipelines = df_jobs["PIPELINE_NAME"].nunique()
    failures = df_jobs[df_jobs["STATUS"] == "FAIL"]["PIPELINE_NAME"].nunique()
    passing = total_pipelines - failures
else:
    total_pipelines = 0
    failures = 0
    passing = 0

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(kpi_card("Total Pipelines", total_pipelines, "#38bdf8"), unsafe_allow_html=True)

with col2:
    st.markdown(kpi_card("Passing Pipelines", passing, "#22c55e"), unsafe_allow_html=True)

with col3:
    st.markdown(kpi_card("Failing Pipelines", failures, "#ef4444"), unsafe_allow_html=True)

st.divider()

# --- 1️⃣ EXECUTION TIMELINESS ---
st.markdown('<div class="section-title">1️⃣ Execution Timeliness</div>', unsafe_allow_html=True)
st.caption("Monitoring job duration, status, and SLA adherence.")

if not df_jobs.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    total_runs = len(df_jobs)
    success_runs = len(df_jobs[df_jobs["STATUS"] == "PASS"])
    fail_runs = len(df_jobs[df_jobs["STATUS"] == "FAIL"])
    sla_breaches = df_jobs["SLA_BREACH"].sum()

    col1.metric("Total Pipeline Runs", total_runs)
    col2.metric("Successful Runs", success_runs, delta=f"{success_runs/total_runs:.0%}" if total_runs > 0 else None)
    col3.metric("Failed Runs", fail_runs, delta=f"-{fail_runs}", delta_color="inverse")
    col4.metric("SLA Breaches", int(sla_breaches), delta=f"-{int(sla_breaches)}" if sla_breaches > 0 else "Stable", delta_color="inverse")

    tab_t1, tab_t2 = st.tabs(["📉 Duration Trend", "📋 Execution Log"])
    
    with tab_t1:
        st.plotly_chart(duration_trend_chart(df_jobs, theme_choice), use_container_width=True)
        
    with tab_t2:
        st.dataframe(
            df_jobs[[
                "PIPELINE_NAME", "JOB_NAME", "STATUS", "JOB_START_TIME", "DURATION_MINUTES", "SLA_BREACH"
            ]].style.applymap(
                lambda v: "color: #22c55e; font-weight:bold;" if v == "PASS" else "color: #ef4444; font-weight:bold;" if v == "FAIL" else ""
                , subset=["STATUS"]
            ),
            use_container_width=True
        )
else:
    st.info("No execution data available.")

st.markdown("---")

# --- 2️⃣ DATA VOLUME & THROUGHPUT ---
st.markdown('<div class="section-title">2️⃣ Data Volume & Throughput</div>', unsafe_allow_html=True)
st.caption("Tracking row counts and data size processed by pipelines.")

if not df_sources.empty:
    total_rows = df_sources["ROW_COUNT"].sum()
    total_gb = df_sources["BYTES"].sum() / (1024**3)
    
    col1, col2 = st.columns(2)
    col1.metric("Total Rows Processed", f"{total_rows:,.0f}")
    col2.metric("Total Data Volume", f"{total_gb:.2f} GB")
    
    col_v1, col_v2 = st.columns([2, 1])
    with col_v1:
        st.plotly_chart(volume_trend_chart(df_sources, theme_choice), use_container_width=True)
    with col_v2:
        st.markdown("### Source Details")
        st.dataframe(
            df_sources[["PIPELINE_NAME", "SOURCE_TABLE", "ROW_COUNT", "BYTES"]],
            use_container_width=True,
            height=300
        )
else:
    st.info("No volume data available.")

st.markdown("---")

# --- 3️⃣ OUTPUT COMPLETENESS ---
st.markdown('<div class="section-title">3️⃣ Output Completeness</div>', unsafe_allow_html=True)
st.caption("Verifying data landing in sink tables.")

if not df_outputs.empty:
    st.dataframe(
        df_outputs[["PIPELINE_NAME", "SINK_TABLE", "ROW_COUNT"]],
        use_container_width=True
    )
else:
    st.info("No output completeness data available.")

st.markdown("---")

# --- 4️⃣ UNIQUENESS & DUPLICATION RISK ---
st.markdown('<div class="section-title">4️⃣ Uniqueness & Duplication Risk</div>', unsafe_allow_html=True)
st.caption("Monitoring duplicate records against defined thresholds.")

if not df_uniqueness.empty:
    # Highlight high risk
    high_risk_dupes = df_uniqueness[df_uniqueness["DUPLICATE_PERCENTAGE"] > df_uniqueness["DUPLICATE_THRESHOLD"]]
    
    if not high_risk_dupes.empty:
        st.error(f"⚠ Detected {len(high_risk_dupes)} pipelines exceeding duplicate thresholds!")
    
    st.dataframe(
        df_uniqueness[[
            "PIPELINE_NAME", "SINK_TABLE", "DUPLICATE_COUNT", "DUPLICATE_PERCENTAGE", "DUPLICATE_THRESHOLD"
        ]].style.apply(
            lambda x: ["background-color: rgba(239, 68, 68, 0.2)"] * len(x) 
            if x["DUPLICATE_PERCENTAGE"] > x["DUPLICATE_THRESHOLD"] 
            else [""] * len(x), 
            axis=1
        ),
        use_container_width=True
    )
else:
    st.info("No uniqueness data available.")

st.markdown("---")

# --- 5️⃣ DATA INTEGRITY ---
st.markdown('<div class="section-title">5️⃣ Data Integrity (Null Monitoring)</div>', unsafe_allow_html=True)
st.caption("Tracking null values in critical columns.")

if not df_integrity.empty:
    total_nulls = df_integrity["NULL_COUNT"].sum()
    st.metric("Total Null Records Detected", f"{total_nulls:,.0f}")
    
    st.dataframe(
        df_integrity[["PIPELINE_NAME", "NULL_COUNT"]],
        use_container_width=True
    )
else:
    st.info("No integrity data available.")
