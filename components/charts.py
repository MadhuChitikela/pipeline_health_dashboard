
import plotly.express as px



def duration_trend_chart(df, theme="dark"):
    # Theme Logic
    if theme == "dark":
        template = "plotly_dark"
        bg_color = "#0E1117"
        font_color = "white"
    else:
        template = "plotly_white"
        bg_color = "#FFFFFF"
        font_color = "black"

    if df.empty:
        fig = px.line(title="Job Duration Trend (No Data)")
    else:
        fig = px.line(
            df,
            x="WEEK",
            y="DURATION_MINUTES",
            color="PIPELINE_NAME",
            title="Job Duration Trend"
        )

    fig.update_layout(
        template=template,
        paper_bgcolor=bg_color,
        plot_bgcolor=bg_color,
        font=dict(color=font_color)
    )
    return fig


def sla_breach_chart(df, theme="dark"):
    # Theme Logic
    if theme == "dark":
        template = "plotly_dark"
        bg_color = "#0E1117"
        font_color = "white"
    else:
        template = "plotly_white"
        bg_color = "#FFFFFF"
        font_color = "black"

    if df.empty:
        fig = px.bar(title="SLA Breach Count (No Data)")
    else:
        agg = df.groupby("PIPELINE_NAME")["SLA_BREACH"].sum().reset_index()
        fig = px.bar(
            agg,
            x="PIPELINE_NAME",
            y="SLA_BREACH",
            title="SLA Breach Count"
        )
    
    fig.update_layout(
        template=template,
        paper_bgcolor=bg_color,
        plot_bgcolor=bg_color,
        font=dict(color=font_color)
    )
    return fig


def health_score_chart(df, theme="dark"):
    # Theme Logic
    if theme == "dark":
        template = "plotly_dark"
        bg_color = "#0E1117"
        font_color = "white"
    else:
        template = "plotly_white"
        bg_color = "#FFFFFF"
        font_color = "black"
        
    if df.empty:
        fig = px.bar(title="Pipeline Health Score (No Data)")
    else:
        fig = px.bar(
            df,
            x="HEALTH_SCORE",
            y="PIPELINE_NAME",
            orientation="h",
            title="Pipeline Health Score"
        )

    fig.update_layout(
        template=template,
        paper_bgcolor=bg_color,
        plot_bgcolor=bg_color,
        font=dict(color=font_color)
    )
    return fig


def volume_trend_chart(df, theme="dark"):
    # Theme Logic
    if theme == "dark":
        template = "plotly_dark"
        bg_color = "#0E1117"
        font_color = "white"
    else:
        template = "plotly_white"
        bg_color = "#FFFFFF"
        font_color = "black"

    if df.empty:
        fig = px.bar(title="Data Volume Trend (No Data)")
    else:
        # Sort by time to ensure trend is correct
        df_sorted = df.sort_values("PIPELINE_START_TIME")
        fig = px.bar(
            df_sorted,
            x="PIPELINE_START_TIME",
            y="ROW_COUNT",
            color="PIPELINE_NAME",
            title="Daily Row Count Processed"
        )

    fig.update_layout(
        template=template,
        paper_bgcolor=bg_color,
        plot_bgcolor=bg_color,
        font=dict(color=font_color)
    )
    return fig
