
import pandas as pd

def standardize_datetimes(df, columns):
    if df.empty: return df
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
    return df


def calculate_duration_minutes(df):
    if df.empty: return df
    df["DURATION_MINUTES"] = (
        (df["END_TIME"] - df["JOB_START_TIME"]).dt.total_seconds() / 60
    )
    return df


def detect_sla_breach(df, threshold=60):
    if df.empty: return df
    df["SLA_BREACH"] = df["DURATION_MINUTES"] > threshold
    return df


def add_week_period(df, column):
    if df.empty: return df
    df["WEEK"] = pd.to_datetime(df[column], utc=True)\
                    .dt.to_period("W")\
                    .astype(str)
    return df


def map_execution_status(df):
    if df.empty: return df
    df["STATUS"] = df["EXECUTION_STATUS"].apply(
        lambda x: "PASS" if str(x).upper().strip() == "SUCCESS" else "FAIL"
    )
    return df
