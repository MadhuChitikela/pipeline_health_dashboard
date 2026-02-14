
import sqlite3
import pandas as pd
import streamlit as st

def get_local_connection():
    return sqlite3.connect("local_simulation.db")

@st.cache_data(ttl=600)
def load_job_timeliness(_session):
    try:
        # 1. Try Snowflake
        if _session:
            return _session.sql("""
                SELECT
                    PIPELINE_NAME,
                    RUN_ID,
                    JOB_NAME,
                    JOB_START_TIME,
                    END_TIME,
                    EXECUTION_STATUS,
                    PIPELINE_START_TIME
                FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_JOB_TIMELINESS
            """).to_pandas()
        
        # 2. Try Local SQLite (Simulation Mode)
        else:
            conn = get_local_connection()
            df = pd.read_sql("SELECT * FROM DIM_PIPELINE_JOB_TIMELINESS", conn)
            conn.close()
            return df

    except Exception as e:
        # Fallback for schema validation if DB missing
        print(f"Loader Error: {e}")
        return pd.DataFrame(columns=[
            "PIPELINE_NAME", "RUN_ID", "JOB_NAME", "JOB_START_TIME", 
            "END_TIME", "EXECUTION_STATUS", "PIPELINE_START_TIME"
        ])


@st.cache_data(ttl=600)
def load_sources(_session):
    try:
        if _session:
            return _session.sql("""
                SELECT
                    RUN_ID,
                    PIPELINE_NAME,
                    SOURCE_TABLE,
                    PIPELINE_START_TIME,
                    ROW_COUNT,
                    BYTES
                FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_SOURCE
            """).to_pandas()
        else:
            conn = get_local_connection()
            df = pd.read_sql("SELECT * FROM DIM_PIPELINE_CONTROL_SOURCE", conn)
            conn.close()
            return df
            
    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "SOURCE_TABLE", 
            "PIPELINE_START_TIME", "ROW_COUNT", "BYTES"
        ])


@st.cache_data(ttl=600)
def load_outputs(_session):
    try:
        if _session:
            return _session.sql("""
                SELECT
                    RUN_ID,
                    PIPELINE_NAME,
                    SINK_TABLE,
                    ROW_COUNT
                FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_OUTPUT_COMPLETENESS
            """).to_pandas()
        else:
            conn = get_local_connection()
            df = pd.read_sql("SELECT * FROM DIM_PIPELINE_CONTROL_OUTPUT_COMPLETENESS", conn)
            conn.close()
            return df
            
    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "SINK_TABLE", "ROW_COUNT"
        ])


@st.cache_data(ttl=600)
def load_uniqueness(_session):
    try:
        if _session:
            return _session.sql("""
                SELECT
                    RUN_ID,
                    PIPELINE_NAME,
                    PIPELINE_START_TIME,
                    SINK_TABLE,
                    DUPLICATE_COUNT,
                    DUPLICATE_PERCENTAGE,
                    DUPLICATE_THRESHOLD
                FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_UNIQUENESS
            """).to_pandas()
        else:
            conn = get_local_connection()
            df = pd.read_sql("SELECT * FROM DIM_PIPELINE_CONTROL_UNIQUENESS", conn)
            conn.close()
            return df

    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "PIPELINE_START_TIME", "SINK_TABLE", 
            "DUPLICATE_COUNT", "DUPLICATE_PERCENTAGE", "DUPLICATE_THRESHOLD"
        ])


@st.cache_data(ttl=600)
def load_integrity(_session):
    try:
        if _session:
            return _session.sql("""
                SELECT
                    RUN_ID,
                    PIPELINE_NAME,
                    PIPELINE_START_TIME,
                    NULL_COUNT
                FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_INTEGRITY
            """).to_pandas()
        else:
            conn = get_local_connection()
            df = pd.read_sql("SELECT * FROM DIM_PIPELINE_CONTROL_INTEGRITY", conn)
            conn.close()
            return df

    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "PIPELINE_START_TIME", "NULL_COUNT"
        ])
