
import streamlit as st

@st.cache_data(ttl=600)
def load_job_timeliness(_session, start_date=None, end_date=None):
    query = """
        SELECT
            PIPELINE_NAME,
            RUN_ID,
            JOB_NAME,
            JOB_START_TIME,
            END_TIME,
            EXECUTION_STATUS,
            PIPELINE_START_TIME
        FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_JOB_TIMELINESS
    """

    if start_date and end_date:
        query += f"""
        WHERE PIPELINE_START_TIME 
        BETWEEN '{start_date}' AND '{end_date}'
        """

    try:
        if _session is None:
            raise Exception("No active session")
        return _session.sql(query).to_pandas()
    except Exception:
        return pd.DataFrame(columns=[
            "PIPELINE_NAME", "RUN_ID", "JOB_NAME", "JOB_START_TIME",
            "END_TIME", "EXECUTION_STATUS", "PIPELINE_START_TIME"
        ])


@st.cache_data(ttl=600)
def load_sources(_session, start_date=None, end_date=None):
    query = """
        SELECT
            RUN_ID,
            PIPELINE_NAME,
            SOURCE_TABLE,
            PIPELINE_START_TIME,
            ROW_COUNT,
            BYTES
        FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_SOURCE
    """

    if start_date and end_date:
        query += f"""
        WHERE PIPELINE_START_TIME 
        BETWEEN '{start_date}' AND '{end_date}'
        """

    try:
        if _session is None:
            raise Exception("No active session")
        return _session.sql(query).to_pandas()
    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "SOURCE_TABLE",
            "PIPELINE_START_TIME", "ROW_COUNT", "BYTES"
        ])


@st.cache_data(ttl=600)
def load_outputs(_session):
    try:
        if _session is None:
            raise Exception("No active session")
        return _session.sql("""
            SELECT
                RUN_ID,
                PIPELINE_NAME,
                SINK_TABLE,
                ROW_COUNT
            FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_OUTPUT_COMPLETENESS
        """).to_pandas()
    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "SINK_TABLE", "ROW_COUNT"
        ])


@st.cache_data(ttl=600)
def load_uniqueness(_session, start_date=None, end_date=None):
    query = """
        SELECT
            RUN_ID,
            PIPELINE_NAME,
            PIPELINE_START_TIME,
            SINK_TABLE,
            DUPLICATE_COUNT,
            DUPLICATE_PERCENT,
            DUPLICATE_THRESHOLD
        FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_UNIQUENESS
    """

    if start_date and end_date:
        query += f"""
        WHERE PIPELINE_START_TIME 
        BETWEEN '{start_date}' AND '{end_date}'
        """

    try:
        if _session is None:
            raise Exception("No active session")
        return _session.sql(query).to_pandas()
    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "PIPELINE_START_TIME", "SINK_TABLE",
            "DUPLICATE_COUNT", "DUPLICATE_PERCENT", "DUPLICATE_THRESHOLD"
        ])


@st.cache_data(ttl=600)
def load_integrity(_session, start_date=None, end_date=None):
    query = """
        SELECT
            RUN_ID,
            PIPELINE_NAME,
            PIPELINE_START_TIME,
            NULL_COUNT
        FROM DB_RETAIL_PRD.CONTROL.DIM_PIPELINE_CONTROL_INTEGRITY
    """

    if start_date and end_date:
        query += f"""
        WHERE PIPELINE_START_TIME 
        BETWEEN '{start_date}' AND '{end_date}'
        """

    try:
        if _session is None:
            raise Exception("No active session")
        return _session.sql(query).to_pandas()
    except Exception:
        return pd.DataFrame(columns=[
            "RUN_ID", "PIPELINE_NAME", "PIPELINE_START_TIME", "NULL_COUNT"
        ])
