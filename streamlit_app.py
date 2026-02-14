
import streamlit as st
try:
    from snowflake.snowpark.context import get_active_session
except ImportError:
    # Fallback for local testing if snowflake-snowpark-python is not fully configured or used outside Snowflake
    def get_active_session():
        # This is a placeholder. In a real scenario, you would create a session manually if needed.
        # Returning None to handle potential errors gracefully
        return None

import pandas as pd
import sys
import os

# Ensure we can import modules if running from root
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_loader import *
from transformations import *
from charts import *
from anomaly import *

st.set_page_config(page_title="Pipeline Health Dashboard", layout="wide")
st.title("Pipeline Health Dashboard")

# --- SESSION HANDLING ---
try:
    session = get_active_session()
except Exception:
    session = None

if session is None:
    # If running locally without Snowflake active session, try to create a manual one or show error
    # For now, we'll try to use a local fallback or just warn
    st.warning("⚠️ No active Snowflake session found. If running locally, data loading may fail.")
    # Attempt to create a local session if configured (optional, based on previous context)
    try:
        from snowflake.snowpark import Session
        # TODO: Configure these for local dev if needed
        # session = Session.builder.configs({...}).create()
        pass
    except:
        pass

# --- 1. SESSION STATE & FILTERS ---
if "kpi_filter" not in st.session_state:
    st.session_state.kpi_filter = "ALL"

def set_filter(status):
    st.session_state.kpi_filter = status

# Date filter
start_date, end_date = st.date_input(
    "Select reporting window",
    value=(
        pd.Timestamp.utcnow().date() - pd.Timedelta(days=56),
        pd.Timestamp.utcnow().date(),
    ),
)

# Time grain
grain_option = st.radio("Time Granularity", ["Weekly", "Daily"], horizontal=True)
grain_code = "W" if grain_option == "Weekly" else "D"

# Load Data
if session:
    with st.spinner("Loading analytics data..."):
        try:
            df_jobs = load_job_timeliness(session, start_date, end_date)
            df_sources = load_sources(session, start_date, end_date)
            df_uniqueness = load_uniqueness(session, start_date, end_date)
            df_integrity = load_integrity(session, start_date, end_date)
        except Exception as e:
            st.error(f"Error loading data: {e}")
            st.stop()
else:
    # Use dummy data if no session (Optional: better to just stop)
    st.info("Please configure Snowflake session to view data.")
    st.stop()

# --- 2. DATA PROCESSING ---
if not df_jobs.empty:
    df_jobs = standardize_datetime(df_jobs, "PIPELINE_START_TIME")
    df_jobs = add_period_column(df_jobs, "PIPELINE_START_TIME", grain_code)
    df_jobs = calculate_duration(df_jobs)
    df_jobs = identify_sla_breaches(df_jobs, threshold_mins=60) # 60 min SLA

if not df_sources.empty:
    df_sources = standardize_datetime(df_sources, "PIPELINE_START_TIME")
    df_sources = add_period_column(df_sources, "PIPELINE_START_TIME", grain_code)

if not df_uniqueness.empty:
    df_uniqueness = standardize_datetime(df_uniqueness, "PIPELINE_START_TIME")
    df_uniqueness = add_period_column(df_uniqueness, "PIPELINE_START_TIME", grain_code)

# Calculate Health Scores
health_scores = calculate_health_score(df_jobs, df_uniqueness)

# --- 3. EXECUTIVE SUMMARY ---
st.markdown("### 📊 Executive Summary")
col_sum1, col_sum2, col_sum3 = st.columns(3)

avg_health = health_scores["Health_Score"].mean() if not health_scores.empty else 0
total_breaches = df_jobs["SLA_BREACH"].sum() if not df_jobs.empty else 0
total_volume = df_sources["BYTES"].sum() / (1024**3) if not df_sources.empty else 0 # GB

col_sum1.metric("Overall System Health", f"{avg_health:.1f}/100", delta=f"{avg_health-90:.1f}" if avg_health < 90 else "Stable")
col_sum2.metric("SLA Breaches (Total)", f"{total_breaches}", delta=f"-{total_breaches}" if total_breaches > 0 else "0", delta_color="inverse")
col_sum3.metric("Data Volume Processed", f"{total_volume:.2f} GB")

st.divider()

# --- 4. INTERACTIVE KPIs ---
total_pipelines = df_jobs["PIPELINE_NAME"].nunique() if not df_jobs.empty else 0
failures = df_jobs[df_jobs["EXECUTION_STATUS"] != "SUCCESS"]["PIPELINE_NAME"].nunique() if not df_jobs.empty else 0
passing = total_pipelines - failures

kpi1, kpi2, kpi3 = st.columns(3)

# Helper for KPI cards (using st.button for interactivity)
with kpi1:
    st.markdown(f"**Total Pipelines**")
    st.markdown(f"<h1 style='margin:0; color:#3b82f6'>{total_pipelines}</h1>", unsafe_allow_html=True)
    if st.button("View All", key="btn_all", use_container_width=True):
        set_filter("ALL")

with kpi2:
    st.markdown(f"**Passing**")
    st.markdown(f"<h1 style='margin:0; color:#22c55e'>{passing}</h1>", unsafe_allow_html=True)
    if st.button("Filter Passing", key="btn_pass", use_container_width=True):
        set_filter("PASS")

with kpi3:
    st.markdown(f"**Critical / Failing**")
    st.markdown(f"<h1 style='margin:0; color:#ef4444'>{failures}</h1>", unsafe_allow_html=True)
    if st.button("Filter Failing", key="btn_fail", use_container_width=True):
        set_filter("FAIL")

# --- 5. FILTER LOGIC ---
st.write(f"**Currently Viewing:** `{st.session_state.kpi_filter}` Pipelines")

filtered_pipelines = df_jobs["PIPELINE_NAME"].unique()
if st.session_state.kpi_filter == "PASS":
    # Get pipelines that NEVER failed in the window (strict) or currently passing? 
    # Let's say pipelines that don't have a failure status in the period
    failing_pipes = df_jobs[df_jobs["EXECUTION_STATUS"] != "SUCCESS"]["PIPELINE_NAME"].unique()
    filtered_pipelines = [p for p in filtered_pipelines if p not in failing_pipes]
elif st.session_state.kpi_filter == "FAIL":
    filtered_pipelines = df_jobs[df_jobs["EXECUTION_STATUS"] != "SUCCESS"]["PIPELINE_NAME"].unique()

# Apply filter to dataframes
df_jobs_view = df_jobs[df_jobs["PIPELINE_NAME"].isin(filtered_pipelines)] if not df_jobs.empty else df_jobs
df_sources_view = df_sources[df_sources["PIPELINE_NAME"].isin(filtered_pipelines)] if not df_sources.empty else df_sources
scores_view = health_scores[health_scores["PIPELINE_NAME"].isin(filtered_pipelines)] if not health_scores.empty else health_scores

# --- 6. CHARTS & GRIDS ---
tab1, tab2, tab3 = st.tabs(["📈 Trends & Volume", "🚨 Health & SLA", "📋 Detailed Data"])

with tab1:
    if not df_sources_view.empty:
        source_weekly = (
            df_sources_view
            .groupby(["Period", "PIPELINE_NAME"])
            .agg(Weekly_Rows=("ROW_COUNT", "sum"))
            .reset_index()
        )
        st.plotly_chart(volume_chart(source_weekly), use_container_width=True)
    
    if not df_jobs_view.empty:
        # Aggregated Duration Trend
        duration_agg = df_jobs_view.groupby(["Period", "PIPELINE_NAME"])["DURATION_MINS"].mean().reset_index()
        st.plotly_chart(duration_trend_chart(duration_agg), use_container_width=True)

with tab2:
    col_h1, col_h2 = st.columns([1, 1])
    with col_h1:
        st.markdown("**Health Scores**")
        if not scores_view.empty:
            st.plotly_chart(health_score_chart(scores_view), use_container_width=True)
            
            with st.expander("How is this calculated?"):
                st.write("Score = (Success Rate * 50%) + (SLA Adherence * 20%) + (Data Quality * 30%)")
    
    with col_h2:
        st.markdown("**SLA Breaches (>60 mins)**")
        if not df_jobs_view.empty:
            st.plotly_chart(sla_breach_chart(df_jobs_view), use_container_width=True)
        
    st.markdown("**Failure Trend**")
    if not df_jobs_view.empty:
        st.plotly_chart(failure_chart(df_jobs_view), use_container_width=True)

with tab3:
    st.subheader("Pipeline Health Details")
    st.dataframe(
        scores_view,
        column_config={
            "Health_Score": st.column_config.ProgressColumn(
                "Health Score",
                help="Composite metric 0-100",
                format="%.1f",
                min_value=0,
                max_value=100,
            ),
        },
        use_container_width=True
    )
    
    st.subheader("Raw Job History")
    st.dataframe(df_jobs_view[["PIPELINE_NAME", "RUN_ID", "JOB_START_TIME", "DURATION_MINS", "EXECUTION_STATUS", "SLA_BREACH"]], use_container_width=True)
