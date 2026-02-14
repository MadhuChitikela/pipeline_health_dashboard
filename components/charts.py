
import plotly.express as px








def apply_theme_to_fig(fig, theme):
    if theme == "dark":
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#ffffff", family="Inter, sans-serif", size=12),
            hoverlabel=dict(bgcolor="#0b1c33", bordercolor="#0096ff", font=dict(color="#ffffff")),
            xaxis=dict(gridcolor="rgba(255,255,255,0.1)"),
            yaxis=dict(gridcolor="rgba(255,255,255,0.1)")
        )
    else:
        fig.update_layout(
            template="plotly_white",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#1b2a41", family="Inter, sans-serif", size=12),
            hoverlabel=dict(bgcolor="#ffffff", bordercolor="#0078ff", font=dict(color="#1b2a41")),
            xaxis=dict(gridcolor="rgba(0,0,0,0.05)"),
            yaxis=dict(gridcolor="rgba(0,0,0,0.05)")
        )
    return fig


def duration_trend_chart(df, theme="dark"):
    if df.empty:
        fig = px.line(title="Job Duration Trend (No Data)")
    else:
        fig = px.line(
            df,
            x="WEEK",
            y="DURATION_MINUTES",
            color="PIPELINE_NAME",
            title="Job Duration Trend",
            color_discrete_sequence=["#3B82F6", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6"]
        )
    return apply_theme_to_fig(fig, theme)


def sla_breach_chart(df, theme="dark"):
    if df.empty:
        fig = px.bar(title="SLA Breach Count (No Data)")
    else:
        agg = df.groupby("PIPELINE_NAME")["SLA_BREACH"].sum().reset_index()
        fig = px.bar(
            agg,
            x="PIPELINE_NAME",
            y="SLA_BREACH",
            title="SLA Breach Count",
            color_discrete_sequence=["#EF4444"]
        )
    return apply_theme_to_fig(fig, theme)


def health_score_chart(df, theme="dark"):
    if df.empty:
        fig = px.bar(title="Pipeline Health Score (No Data)")
    else:
        fig = px.bar(
            df,
            x="HEALTH_SCORE",
            y="PIPELINE_NAME",
            orientation="h",
            title="Pipeline Health Score",
            color="HEALTH_SCORE",
            color_continuous_scale="RdYlGn"
        )
    return apply_theme_to_fig(fig, theme)


def volume_trend_chart(df, theme="dark"):
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
            title="Daily Row Count Processed",
            color_discrete_sequence=["#3B82F6", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6"]
        )
    return apply_theme_to_fig(fig, theme)
