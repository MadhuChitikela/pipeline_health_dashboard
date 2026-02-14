
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

st.set_page_config(
    page_title="Pipeline Operations Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---- Custom CSS ----
st.markdown("""
<style>

/* Page background */
.stApp {
    background-color: #0e1117;
    color: white;
}

/* Section headers */
.section-title {
    font-size: 26px;
    font-weight: 600;
    color: #4ea8de;
    margin-bottom: 10px;
}

/* KPI Card */
.kpi-card {
    background-color: #161b22;
    padding: 25px;
    border-radius: 12px;
    box-shadow: 0px 4px 15px rgba(0,0,0,0.4);
    border-left: 6px solid #4ea8de;
    margin-bottom: 10px;
}

.kpi-value {
    font-size: 40px;
    font-weight: bold;
    color: #ffffff;
}

.kpi-title {
    font-size: 14px;
    text-transform: uppercase;
    color: #9ca3af;
    font-weight: 600;
}

/* Buttons */
.stButton>button {
    border-radius: 25px;
    height: 45px;
    font-weight: 600;
    background: linear-gradient(90deg,#4ea8de,#3a86ff);
    color: white;
    border: none;
    width: 100%;
}

.stButton>button:hover {
    transform: scale(1.02);
    box-shadow: 0px 5px 15px rgba(58, 134, 255, 0.4);
}

/* Table styling */
[data-testid="stDataFrame"] {
    background-color: #161b22;
    border-radius: 10px;
    padding: 10px;
}

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

# Check Environment
if session:
    DEMO_MODE = False
else:
    DEMO_MODE = True

# --- Load Data (Strict Demo Mode or Production) ---
if DEMO_MODE:
    st.warning("⚠ Running in STRICT DEMO MODE (Snowflake session not detected). Using synthetic schema-compliant data.")
    
    # Generate Synthetic Data for Local Demo
    now = datetime.utcnow()
    
    # 1. Jobs Data (Timeliness & SLA)
    df_jobs = pd.DataFrame({
        "PIPELINE_NAME": ["PIPE_SALES", "PIPE_SALES", "PIPE_INVENTORY", "PIPE_CUSTOMERS", "PIPE_FINANCE"],
        "RUN_ID": [101, 105, 102, 103, 104],
        "JOB_NAME": ["LOAD_SALES", "LOAD_SALES", "LOAD_INV", "LOAD_CUST", "LOAD_FIN"],
        "JOB_START_TIME": [
            now - timedelta(minutes=45),
            now - timedelta(days=1, minutes=30), 
            now - timedelta(minutes=90),
            now - timedelta(hours=5),
            now - timedelta(days=2)
        ],
        "END_TIME": [
            now - timedelta(minutes=15),    # 30 min duration
            now - timedelta(days=1, minutes=5), # 25 min duration
            now - timedelta(minutes=10),    # 80 min duration (SLA BREACH > 60)
            now - timedelta(hours=4),       # 60 min duration
            now - timedelta(days=2, minutes=10) # FAILED job
        ],
        "EXECUTION_STATUS": ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED"],
        "PIPELINE_START_TIME": [
            now - timedelta(days=0),
            now - timedelta(days=1),
            now - timedelta(days=0),
            now - timedelta(days=0),
            now - timedelta(days=2)
        ]
    })

    # 2. Source Data (Volume)
    df_sources = pd.DataFrame({
        "RUN_ID": [101, 105, 102, 103, 104],
        "PIPELINE_NAME": ["PIPE_SALES", "PIPE_SALES", "PIPE_INVENTORY", "PIPE_CUSTOMERS", "PIPE_FINANCE"],
        "SOURCE_TABLE": ["SRC_SALES", "SRC_SALES", "SRC_INV", "SRC_CUST", "SRC_FIN"],
        "PIPELINE_START_TIME": [
            now - timedelta(days=0),
            now - timedelta(days=1),
            now - timedelta(days=0),
            now - timedelta(days=0),
            now - timedelta(days=2)
        ],
        "ROW_COUNT": [150000, 145000, 50000, 200000, 10000],
        "BYTES": [10485760, 10240000, 5242880, 20971520, 102400]
    })

    # 3. Output Data (Completeness)
    df_outputs = pd.DataFrame({
        "RUN_ID": [101, 105, 102, 103, 104],
        "PIPELINE_NAME": ["PIPE_SALES", "PIPE_SALES", "PIPE_INVENTORY", "PIPE_CUSTOMERS", "PIPE_FINANCE"],
        "SINK_TABLE": ["FACT_SALES", "FACT_SALES", "FACT_INVENTORY", "DIM_CUSTOMERS", "FACT_FINANCE"],
        "ROW_COUNT": [150000, 145000, 50000, 200000, 0] # 0 rows for failed job
    })

    # 4. Uniqueness (Data Quality)
    df_uniqueness = pd.DataFrame({
        "RUN_ID": [101, 105, 102, 103, 104],
        "PIPELINE_NAME": ["PIPE_SALES", "PIPE_SALES", "PIPE_INVENTORY", "PIPE_CUSTOMERS", "PIPE_FINANCE"],
        "PIPELINE_START_TIME": [
            now - timedelta(days=0),
            now - timedelta(days=1),
            now - timedelta(days=0),
            now - timedelta(days=0),
            now - timedelta(days=2)
        ],
        "SINK_TABLE": ["FACT_SALES", "FACT_SALES", "FACT_INVENTORY", "DIM_CUSTOMERS", "FACT_FINANCE"],
        "DUPLICATE_COUNT": [0, 0, 120, 50, 0],
        # Handling schema variation (PERCENT vs PERCENTAGE)
        "DUPLICATE_PERCENTAGE": [0.0, 0.0, 2.5, 0.5, 0.0], 
        "DUPLICATE_THRESHOLD": [1.0, 1.0, 1.0, 1.0, 1.0]
    })
    # If using newer schema logic which expects DUPLICATE_PERCENT
    df_uniqueness["DUPLICATE_PERCENT"] = df_uniqueness["DUPLICATE_PERCENTAGE"]

    # 5. Integrity (Nulls)
    df_integrity = pd.DataFrame({
        "RUN_ID": [101, 105, 102, 103, 104],
        "PIPELINE_NAME": ["PIPE_SALES", "PIPE_SALES", "PIPE_INVENTORY", "PIPE_CUSTOMERS", "PIPE_FINANCE"],
        "PIPELINE_START_TIME": [
            now - timedelta(days=0),
            now - timedelta(days=1),
            now - timedelta(days=0),
            now - timedelta(days=0),
            now - timedelta(days=2)
        ],
        "NULL_COUNT": [5, 2, 240, 10, 0]
    })

else:
    # Production Execution (Snowflake)
    try:
        df_jobs = load_job_timeliness(session)
        df_sources = load_sources(session)
        df_outputs = load_outputs(session)
        df_uniqueness = load_uniqueness(session)
        df_integrity = load_integrity(session)
    except Exception as e:
        st.error(f"Critical Error loading production data: {e}")
        st.stop()

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
    if st.button("View All", key="total_btn"):
        pass # Add callback logic if needed

with col2:
    st.markdown(kpi_card("Passing Pipelines", passing, "#22c55e"), unsafe_allow_html=True)
    if st.button("Filter Passing", key="pass_btn"):
        pass

with col3:
    st.markdown(kpi_card("Failing Pipelines", failures, "#ef4444"), unsafe_allow_html=True)
    if st.button("Filter Failing", key="fail_btn"):
        pass

st.divider()

# --- MAIN LAYOUT ---
col_left, col_right = st.columns([2, 1])

with col_left:
    st.markdown('<div class="section-title">Execution Overview</div>', unsafe_allow_html=True)
    
    # Styled DataFrame
    if not df_jobs.empty:
        # Select relevant stats
        display_df = df_jobs[[
            "PIPELINE_NAME", "RUN_ID", "JOB_START_TIME", "DURATION_MINUTES", "STATUS", "SLA_BREACH"
        ]].copy()
        
        # Color styling
        st.dataframe(
            display_df.style.applymap(
                lambda v: "color: #22c55e; font-weight:bold;" if v == "PASS" 
                else "color: #ef4444; font-weight:bold;" if v == "FAIL" 
                else ""
                , subset=["STATUS"]
            ).applymap(
                lambda v: "color: #ef4444; font-weight:bold;" if v == True 
                else "color: #94a3b8;" if v == False 
                else ""
                , subset=["SLA_BREACH"]
            ).format({
                "DURATION_MINUTES": "{:.1f} m"
            }),
            use_container_width=True,
            height=300
        )
    else:
        st.info("No execution data available.")

with col_right:
    st.markdown('<div class="section-title">Pipeline Health Trend</div>', unsafe_allow_html=True)
    if not df_jobs.empty:
        st.plotly_chart(duration_trend_chart(df_jobs), use_container_width=True)
    else:
        st.info("No trend data.")

st.markdown("---")

# --- BOTTOM SECTION ---
st.markdown('<div class="section-title">Weekly Performance Insights</div>', unsafe_allow_html=True)

colA, colB = st.columns([1,1])

with colA:
    st.markdown("### Data Quality Risk & Stability")
    if not df_uniqueness.empty:
         st.dataframe(df_uniqueness[["PIPELINE_NAME", "SINK_TABLE", "DUPLICATE_COUNT", "DUPLICATE_PERCENT"]], use_container_width=True)
    else:
        st.info("No quality data.")

with colB:
    st.markdown("### SLA Breaches")
    if not df_jobs.empty:
        st.plotly_chart(sla_breach_chart(df_jobs), use_container_width=True)
    else:
        st.info("No SLA data.")
