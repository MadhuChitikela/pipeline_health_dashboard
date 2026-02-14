
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

# ---- DYNAMIC CSS (Final Fix - Force Table & Glow) ----
if st.session_state.theme_mode == "dark":
    bg_color = "#0B1426"
    text_color = "#FFFFFF"
    card_bg = "#111C33"
    border_color = "#1F2937"
    metric_bg = "#111C33"
    header_color = "#FFFFFF"
    
    # --- DARK MODE STYLES ---
    st.markdown("""
    <style>
    /* FULL PAGE BACKGROUND */
    .stApp, section[data-testid="stAppViewContainer"], section.main > div {
        background-color: #0B1426 !important;
        color: #FFFFFF !important;
    }

    /* ALL TEXT FORCE WHITE */
    h1, h2, h3, h4, h5, h6, .section-title,
    p, span, div, label, li {
        color: #FFFFFF !important;
    }

    /* Section Subtitles */
    .section-subtitle {
        color: #E6EDF3 !important;
        opacity: 0.9;
    }

    /* Metric Containers - Gradient & Shadow */
    div[data-testid="stMetric"], div[data-testid="metric-container"] {
        background: linear-gradient(145deg, #101C33, #0E1A30) !important;
        border: 1px solid rgba(0,150,255,0.3) !important;
        border-radius: 14px !important;
        padding: 20px !important;
        box-shadow: 0 0 25px rgba(0,120,255,0.2) !important;
    }

    /* Metric Values - Glow */
    div[data-testid="stMetricValue"], div[data-testid="metric-container"] div {
        color: #FFFFFF !important;
        font-size: 28px !important;
        font-weight: 700 !important;
        text-shadow: 0 0 12px rgba(0,150,255,0.9) !important;
    }

    /* Section Headings Glow */
    h1, h2, h3, .section-title {
        color: #FFFFFF !important;
        text-shadow: 0 0 8px rgba(0,150,255,0.7) !important;
    }

    /* ===== FORCE DARK TABLE ===== */
    div[data-testid="stDataFrame"] {
        background-color: #0F1C33 !important;
        border-radius: 14px !important;
        box-shadow: 0 0 25px rgba(0,150,255,0.25);
        border: 1px solid rgba(0,150,255,0.2);
    }

    div[data-testid="stDataFrame"] table {
        background-color: #0F1C33 !important;
        color: white !important;
    }

    div[data-testid="stDataFrame"] thead tr th {
        background-color: #16284B !important;
        color: #FFFFFF !important;
        font-weight: 700 !important;
        border-bottom: 2px solid #2D3748 !important;
    }

    div[data-testid="stDataFrame"] tbody tr td {
        background-color: #0F1C33 !important;
        color: #FFFFFF !important;
        border-bottom: 1px solid #1F2937 !important;
    }

    div[data-testid="stDataFrame"] tbody tr:nth-child(even) td {
        background-color: #142241 !important;
    }

    /* Buttons */
    .stButton > button {
        background-color: #2563EB;
        color: #FFFFFF !important;
        border-radius: 8px;
        border: none;
        font-weight: 600;
        box-shadow: 0 4px 14px rgba(37, 99, 235, 0.3);
    }
    .stButton > button:hover {
        background-color: #3B82F6;
        box-shadow: 0 6px 20px rgba(59, 130, 246, 0.5);
    }
    </style>
    """, unsafe_allow_html=True)

else:
    # --- LIGHT MODE STYLES (Glass + Soft Glow) ---
    bg_color = "#F1F5F9"
    text_color = "#0F172A"
    card_bg = "rgba(255, 255, 255, 0.8)"
    border_color = "#CBD5E1"
    metric_bg = "#FFFFFF"
    header_color = "#2563EB"
    
    st.markdown("""
    <style>
    /* Main Background */
    .stApp, section[data-testid="stAppViewContainer"] {
        background-color: #F1F5F9;
    }

    /* Global Text */
    section[data-testid="stAppViewContainer"] *, 
    section[data-testid="stAppViewContainer"] p, 
    section[data-testid="stAppViewContainer"] span, 
    section[data-testid="stAppViewContainer"] label, 
    section[data-testid="stAppViewContainer"] li {
        color: #0F172A !important;
    }

    /* Headings */
    h1, h2, h3, h4, h5, .section-title {
        color: #0A1F44 !important;
        text-shadow: 0 0 6px rgba(0,120,255,0.4) !important;
        font-weight: 700 !important;
    }

    /* Subtitles */
    .section-subtitle {
        color: #475569 !important;
        font-weight: 400;
    }

    /* Glass KPI */
    div[data-testid="stMetric"], div[data-testid="metric-container"] {
        background: rgba(255,255,255,0.6) !important;
        backdrop-filter: blur(12px);
        border-radius: 14px !important;
        border: 1px solid rgba(0,120,255,0.2) !important;
        box-shadow: 0 0 20px rgba(0,120,255,0.15) !important;
    }
    
    /* Metric Values */
    div[data-testid="stMetricValue"] {
        color: #2563EB !important;
        font-size: 28px !important;
        font-weight: 700 !important;
    }

    /* Metric Labels */
    div[data-testid="stMetricLabel"] label {
        color: #64748B !important;
        font-weight: 600;
    }

    /* Light Table */
    div[data-testid="stDataFrame"] {
        background: rgba(255,255,255,0.8) !important;
        backdrop-filter: blur(10px);
        border-radius: 14px;
        border: 1px solid #E2E8F0;
    }
    div[data-testid="stDataFrame"] thead tr th {
        background-color: #EAF4FF !important;
        color: #0A1F44 !important;
        border-bottom: 2px solid #E2E8F0 !important;
    }
    div[data-testid="stDataFrame"] tbody td {
        background-color: transparent !important;
        color: #0A1F44 !important;
    }
    div[data-testid="stDataFrame"] tbody tr:nth-of-type(even) {
        background-color: rgba(248, 250, 252, 0.5) !important;
    }

    /* Buttons */
    .stButton > button {
        background-color: #E2E8F0;
        color: #0F172A !important;
        border-radius: 8px;
        border: 1px solid #CBD5E1;
        font-weight: 600;
    }
    .stButton > button:hover {
        background-color: #CBD5E1;
    }
    </style>
    """, unsafe_allow_html=True)

# Helper for Enhanced Table Styling
def style_table(df, theme):
    if theme == "dark":
        return df.style.set_table_styles(
            [{"selector": "th",
              "props": [
                  ("background-color", "#16284B"), 
                  ("color", "#FFFFFF"), 
                  ("font-weight", "700"),
                  ("border-bottom", "2px solid #2D3748")
              ]}]
        ).set_properties(**{
            "background-color": "#0F1C33",
            "color": "#FFFFFF",
            "border-color": "#1F2937"
        })
    else:
        return df.style.set_table_styles(
            [{"selector": "th",
              "props": [
                  ("background-color", "#EAF4FF"), 
                  ("color", "#0A1F44"), 
                  ("font-weight", "600"),
                  ("border-bottom", "2px solid #E2E8F0")
              ]}]
        ).set_properties(**{
            "background-color": "#FFFFFF",
            "color": "#0A1F44",
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
st.markdown("""
<div class="section-title">Pipeline Operations Dashboard</div>
<div class="section-subtitle">Monitor Execution Health • Detect Failures • Track Data Quality</div>
""", unsafe_allow_html=True)
st.markdown("---")


# --- HELPER COMPONENT ---
# --- 1️⃣ EXECUTION TIMELINESS ---
st.markdown("""
<div class="section-title">1️⃣ Execution Timeliness</div>
<div class="section-subtitle">Audit of job duration, execution status, and SLA adherence based on DIM_PIPELINE_JOB_TIMELINESS.</div>
""", unsafe_allow_html=True)

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
st.markdown("""
<div class="section-title">2️⃣ Data Volume & Throughput</div>
<div class="section-subtitle">Tracking row counts and data size processed by pipelines.</div>
""", unsafe_allow_html=True)

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
st.markdown("""
<div class="section-title">3️⃣ Output Completeness</div>
<div class="section-subtitle">Verifying data landing in sink tables.</div>
""", unsafe_allow_html=True)

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
st.markdown("""
<div class="section-title">4️⃣ Uniqueness & Duplication Risk</div>
<div class="section-subtitle">Monitoring duplicate records against defined thresholds.</div>
""", unsafe_allow_html=True)

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
st.markdown("""
<div class="section-title">5️⃣ Data Integrity (Null Monitoring)</div>
<div class="section-subtitle">Tracking null values in critical columns.</div>
""", unsafe_allow_html=True)

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
