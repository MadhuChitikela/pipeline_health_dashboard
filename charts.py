
import plotly.express as px

def volume_chart(df):
    fig = px.line(
        df,
        x="Period",
        y="Weekly_Rows",
        color="PIPELINE_NAME",
        title="Volume Trend"
    )
    fig.update_layout(height=400)
    return fig


def failure_chart(df):
    fail_df = df[df["EXECUTION_STATUS"] != "SUCCESS"]

    agg = (
        fail_df
        .groupby(["Period", "PIPELINE_NAME"])
        .size()
        .reset_index(name="Failure_Count")
    )

    fig = px.line(
        agg,
        x="Period",
        y="Failure_Count",
        color="PIPELINE_NAME",
        title="Failure Trend"
    )
    return fig


def data_quality_chart(df):
    # Determine the correct column name dynamically or assume the standard one
    col_name = "DUPLICATE_PERCENT" if "DUPLICATE_PERCENT" in df.columns else "DUPLICATE_PERCENTAGE"
    
    fig = px.line(
        df,
        x="Period",
        y=col_name,
        color="PIPELINE_NAME",
        title="Duplicate Percentage Trend"
    )
    return fig

def duration_trend_chart(df):
    fig = px.bar(
        df,
        x="Period",
        y="DURATION_MINS",
        color="PIPELINE_NAME",
        barmode="group",
        title="Avg Job Duration (Mins)"
    )
    return fig

def sla_breach_chart(df):
    """Bar chart showing number of SLA breaches per pipeline"""
    breaches = df[df["SLA_BREACH"]].groupby("PIPELINE_NAME").size().reset_index(name="Breach_Count")
    
    if breaches.empty:
        # Return empty figure with layout
        return px.bar(title="SLA Breaches (None Detected)")

    fig = px.bar(
        breaches,
        x="PIPELINE_NAME",
        y="Breach_Count",
        color="PIPELINE_NAME",
        title="SLA Breaches (Duration > 60m)"
    )
    return fig

def health_score_chart(scores_df):
    """Horizontal bar chart for health scores"""
    fig = px.bar(
        scores_df,
        x="Health_Score",
        y="PIPELINE_NAME",
        orientation='h',
        color="Health_Score",
        color_continuous_scale=["red", "yellow", "green"],
        range_color=[0, 100],
        title="Pipeline Health Score (0-100)"
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    return fig
