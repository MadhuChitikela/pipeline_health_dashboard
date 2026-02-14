
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

# ---- THEME STATE MANAGEMENT ----
if "theme_mode" not in st.session_state:
    st.session_state.theme_mode = "dark"

# ---- HEADER & TOGGLE ----
header_left, header_right = st.columns([9,1])

with header_left:
    st.title("Pipeline Health Dashboard")

with header_right:
    # Improved Button with Label
    btn_label = "🌙 Dark" if st.session_state.theme_mode == "light" else "☀ Light"
    if st.button(btn_label, key="theme_toggle", help="Toggle Theme"):
        st.session_state.theme_mode = "dark" if st.session_state.theme_mode == "light" else "light"
        st.rerun()

# Set compatibility variable for charts (lowercase for consistency)
theme_choice = "dark" if st.session_state.theme_mode == "dark" else "light"

# ---- DYNAMIC CSS (Force Override) ----
if st.session_state.theme_mode == "dark":
    bg_color = "#0B1220"
    text_color = "#E6EDF3"
    card_bg = "#111827"
    border_color = "#1F2937"
    metric_bg = "#111827"
else:
    bg_color = "#F1F5F9"
    text_color = "#0F172A"
    card_bg = "rgba(255, 255, 255, 0.75)"
    border_color = "#CBD5E1"
    metric_bg = "#FFFFFF"

st.markdown(f"""
<style>
    /* Main App Background - Targeted Specificity */
    .stApp, section[data-testid="stAppViewContainer"] {{
        background-color: {bg_color};
    }}

    /* Global Text Override */
    section[data-testid="stAppViewContainer"] *, 
    section[data-testid="stAppViewContainer"] p,
    section[data-testid="stAppViewContainer"] span,
    section[data-testid="stAppViewContainer"] label,
    section[data-testid="stAppViewContainer"] h1,
    section[data-testid="stAppViewContainer"] h2,
    section[data-testid="stAppViewContainer"] h3 {{
        color: {text_color} !important;
    }}

    /* Metric Cards - High Specificity */
    div[data-testid="stMetric"] {{
        background-color: {metric_bg} !important;
        border: 1px solid {border_color};
        border-radius: 12px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
    }}

    /* Metric Labels */
    div[data-testid="stMetricLabel"] label {{
        color: {text_color} !important;
        opacity: 0.7;
        font-weight: 600;
    }}

    div[data-testid="stMetricValue"] {{
        color: {text_color} !important;
    }}

    /* Buttons */
    .stButton > button {{
        background-color: {border_color};
        color: {text_color};
        border-radius: 8px;
        border: none;
        font-weight: 600;
        transition: all 0.2s;
    }}
    .stButton > button:hover {{
        filter: brightness(1.1);
    }}

    /* DataTable Container */
    [data-testid="stDataFrame"] {{
        background-color: {card_bg};
        border: 1px solid {border_color};
        border-radius: 10px;
        padding: 5px;
    }}

</style>
""", unsafe_allow_html=True)

# Helper for Enhanced Table Styling
def style_table(df, theme):
    if theme == "dark":
        return df.style.set_table_styles(
            [{"selector": "th",
              "props": [("background-color", "#161B22"), ("color", "#E6EDF3"), ("border-bottom", "2px solid #30363D")]}]
        ).set_properties(**{
            "background-color": "#0D1117",
            "color": "#E6EDF3",
            "border-color": "#30363D"
        })
    else:
        return df.style.set_table_styles(
            [{"selector": "th",
              "props": [("background-color", "#E3F2FD"), ("color", "#1E293B"), ("border-bottom", "2px solid #BFDBFE")]}]
        ).set_properties(**{
            "background-color": "rgba(255, 255, 255, 0.8)",
            "color": "#1E293B",
            "border-color": "#E2E8F0"
        })



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
# --- 1️⃣ EXECUTION TIMELINESS ---
st.markdown('<div class="section-title">1️⃣ Execution Timeliness</div>', unsafe_allow_html=True)
st.caption("Audit of job duration, execution status, and SLA adherence based on DIM_PIPELINE_JOB_TIMELINESS.")

if not df_jobs.empty:
    # Strict Metrics: Raw Counts
    total_runs = len(df_jobs)
    distinct_pipelines = df_jobs["PIPELINE_NAME"].nunique()
    # Using raw status counts
    success_runs = len(df_jobs[df_jobs["STATUS"] == "PASS"])
    fail_runs = len(df_jobs[df_jobs["STATUS"] == "FAIL"])
    sla_breaches = df_jobs["SLA_BREACH"].sum()

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Distinct Pipelines", distinct_pipelines)
    m2.metric("Total Job Runs", total_runs)
    m3.metric("Success Runs", success_runs)
    m4.metric("Failed Runs", fail_runs)
    m5.metric("SLA Breaches", int(sla_breaches))

    tab_t1, tab_t2 = st.tabs(["📉 Duration Trend", "📋 Raw Execution Log"])
    
    with tab_t1:
        st.plotly_chart(duration_trend_chart(df_jobs, theme_choice), use_container_width=True)
        
    with tab_t2:
        # Displaying raw fields + computed duration for audit
        st.dataframe(
            style_table(
                df_jobs[[
                    "PIPELINE_NAME", "JOB_NAME", "EXECUTION_STATUS", "JOB_START_TIME", "END_TIME", "DURATION_MINUTES", "SLA_BREACH"
                ]], 
                theme_choice
            ).map(
                lambda v: "color: #ef4444; font-weight:bold;" if v == True else ""
                , subset=["SLA_BREACH"]
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
            style_table(
                df_sources[["PIPELINE_NAME", "SOURCE_TABLE", "ROW_COUNT", "BYTES"]],
                theme_choice
            ),
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
        style_table(
            df_outputs[["PIPELINE_NAME", "SINK_TABLE", "ROW_COUNT"]],
            theme_choice
        ),
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
        style_table(
            df_uniqueness[[
                "PIPELINE_NAME", "SINK_TABLE", "DUPLICATE_COUNT", "DUPLICATE_PERCENTAGE", "DUPLICATE_THRESHOLD"
            ]],
            theme_choice
        ).apply(
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
        style_table(
            df_integrity[["PIPELINE_NAME", "NULL_COUNT"]],
            theme_choice
        ),
        use_container_width=True
    )
else:
    st.info("No integrity data available.")
