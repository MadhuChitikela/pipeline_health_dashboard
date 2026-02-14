
import pandas as pd

def standardize_datetime(df, col):
    df[col] = pd.to_datetime(df[col], utc=True)
    return df

def add_period_column(df, col, grain="W"):
    if grain == "W":
        df["Period"] = pd.to_datetime(df[col], utc=True).dt.to_period("W").astype(str)
    else:
        df["Period"] = pd.to_datetime(df[col], utc=True).dt.date
    return df

def map_status(value):
    return "PASS" if str(value).upper().strip() == "SUCCESS" else "FAIL"

def calculate_duration(df):
    """Calculates job duration in minutes."""
    if df.empty:
        return df
    # Ensure datetime format
    df["JOB_START_TIME"] = pd.to_datetime(df["JOB_START_TIME"], utc=True)
    df["END_TIME"] = pd.to_datetime(df["END_TIME"], utc=True)
    
    df["DURATION_MINS"] = (df["END_TIME"] - df["JOB_START_TIME"]).dt.total_seconds() / 60
    return df

def identify_sla_breaches(df, threshold_mins=60):
    """Flags jobs that exceeded the duration threshold."""
    if "DURATION_MINS" not in df.columns:
        df = calculate_duration(df)
    
    df["SLA_BREACH"] = df["DURATION_MINS"] > threshold_mins
    return df

def calculate_health_score(df_jobs, df_quality):
    """
    Calculates a 0-100 health score per pipeline based on:
    - Success Rate (50%)
    - SLA Adherence (20%)
    - Data Quality (Uniqueness) (30%)
    """
    if df_jobs.empty:
        return pd.DataFrame()

    # 1. Success Score
    success_stats = df_jobs.groupby("PIPELINE_NAME").agg(
        Total_Runs=("RUN_ID", "count"),
        Success_Runs=("EXECUTION_STATUS", lambda x: (x == "SUCCESS").sum())
    ).reset_index()
    success_stats["Success_Rate"] = success_stats["Success_Runs"] / success_stats["Total_Runs"]

    # 2. SLA Score
    df_jobs = identify_sla_breaches(df_jobs)
    sla_stats = df_jobs.groupby("PIPELINE_NAME").agg(
        SLA_Breaches=("SLA_BREACH", "sum")
    ).reset_index()
    sla_stats = sla_stats.merge(success_stats[["PIPELINE_NAME", "Total_Runs"]], on="PIPELINE_NAME")
    sla_stats["SLA_Score"] = 1 - (sla_stats["SLA_Breaches"] / sla_stats["Total_Runs"])

    # 3. Quality Score (Inverse of Duplicate %)
    if not df_quality.empty:
        # Check column name availability
        col_name = "DUPLICATE_PERCENT" if "DUPLICATE_PERCENT" in df_quality.columns else "DUPLICATE_PERCENTAGE"
        
        quality_stats = df_quality.groupby("PIPELINE_NAME")[col_name].mean().reset_index()
        quality_stats["Quality_Score"] = 1 - (quality_stats[col_name] / 100)
    else:
        quality_stats = pd.DataFrame({"PIPELINE_NAME": success_stats["PIPELINE_NAME"], "Quality_Score": 1.0})

    # Merge
    scores = success_stats.merge(sla_stats[["PIPELINE_NAME", "SLA_Score"]], on="PIPELINE_NAME", how="left")
    scores = scores.merge(quality_stats, on="PIPELINE_NAME", how="left")
    
    # Fill N/A with perfect scores if missing data
    scores = scores.fillna(1.0)

    # Weighted Score
    # Success: 50%, SLA: 20%, Quality: 30%
    scores["Health_Score"] = (
        (scores["Success_Rate"] * 50) + 
        (scores["SLA_Score"] * 20) + 
        (scores["Quality_Score"] * 30)
    ).round(1)

    return scores.sort_values("Health_Score")
