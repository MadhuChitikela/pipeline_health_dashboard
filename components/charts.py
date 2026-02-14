
import plotly.express as px


def duration_trend_chart(df, theme="Dark"):
    template = "plotly_dark" if theme == "Dark" else "plotly_white"
    if df.empty:
        return px.line(title="Job Duration Trend (No Data)", template=template)
    
    fig = px.line(
        df,
        x="WEEK",
        y="DURATION_MINUTES",
        color="PIPELINE_NAME",
        title="Job Duration Trend",
        template=template
    )
    return fig


def sla_breach_chart(df, theme="Dark"):
    template = "plotly_dark" if theme == "Dark" else "plotly_white"
    if df.empty:
        return px.bar(title="SLA Breach Count (No Data)", template=template)
        
    agg = df.groupby("PIPELINE_NAME")["SLA_BREACH"].sum().reset_index()

    fig = px.bar(
        agg,
        x="PIPELINE_NAME",
        y="SLA_BREACH",
        title="SLA Breach Count",
        template=template
    )
    return fig


def health_score_chart(df, theme="Dark"):
    template = "plotly_dark" if theme == "Dark" else "plotly_white"
    if df.empty:
        return px.bar(title="Pipeline Health Score (No Data)", template=template)
        
    fig = px.bar(
        df,
        x="HEALTH_SCORE",
        y="PIPELINE_NAME",
        orientation="h",
        title="Pipeline Health Score",
        template=template
    )
    return fig


def volume_trend_chart(df, theme="Dark"):
    template = "plotly_dark" if theme == "Dark" else "plotly_white"
    if df.empty:
        return px.bar(title="Data Volume Trend (No Data)", template=template)
    
    # Ensure date/time sorting if needed, but assuming df is passed correctly from app
    # Group by PERIOD if available or just date
    
    fig = px.bar(
        df,
        x="PIPELINE_START_TIME",
        y="ROW_COUNT",
        color="PIPELINE_NAME",
        title="Daily Row Count Processed",
        template=template
    )
    return fig
