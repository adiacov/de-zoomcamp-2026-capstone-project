import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure

# ── Theme palettes ────────────────────────────────────────────────────────────

DARK_THEME = {
    "bg_color": "#0e1117",
    "paper_color": "#0e1117",
    "grid_color": "#2a2d35",
    "font_color": "#e0e0e0",
    "customer_colors": {
        "member": "#4C9BE8",
        "casual": "#F4845F",
    },
    "station_colors": {
        "start": "#4C9BE8",
        "end": "#F4845F",
    },
    "duration_colors": {
        "Average": "#4C9BE8",
        "Median": "#A78BFA",
    },
    "legend": {
        "bgcolor": "rgba(255,255,255,0.04)",
        "bordercolor": "rgba(255,255,255,0.08)",
    },
}

LIGHT_THEME = {
    "bg_color": "#ffffff",
    "paper_color": "#ffffff",
    "grid_color": "#e5e7eb",
    "font_color": "#1a1a2e",
    "customer_colors": {
        "member": "#2563EB",
        "casual": "#EA580C",
    },
    "station_colors": {
        "start": "#2563EB",
        "end": "#EA580C",
    },
    "duration_colors": {
        "Average": "#2563EB",
        "Median": "#7C3AED",
    },
    "legend": {
        "bgcolor": "rgba(0,0,0,0.03)",
        "bordercolor": "rgba(0,0,0,0.10)",
    },
}

FONT_FAMILY = "Inter, Arial, sans-serif"


# ── Theme detection ───────────────────────────────────────────────────────────


def _get_theme() -> dict:
    """
    Detect the active Streamlit theme at runtime.
    Falls back to DARK_THEME in notebooks or plain Python execution.
    """
    try:
        import streamlit as st

        base = st.get_option("theme.base")  # "dark" | "light" | None
        return LIGHT_THEME if base == "light" else DARK_THEME
    except Exception:
        return DARK_THEME


# ── Apply theme ───────────────────────────────────────────────────────────────


def _apply_theme(fig: Figure, *, title: str = "") -> Figure:
    """Apply the detected theme to any Plotly figure."""
    t = _get_theme()

    fig.update_layout(
        plot_bgcolor=t["bg_color"],
        paper_bgcolor=t["paper_color"],
        title=dict(
            text=title or fig.layout.title.text,
            font=dict(
                size=17, color=t["font_color"], family=FONT_FAMILY, weight="bold"
            ),
            x=0.5,
            xanchor="center",
            pad=dict(b=12),
        ),
        font=dict(color=t["font_color"], family=FONT_FAMILY, size=12),
        legend=dict(
            bgcolor=t["legend"]["bgcolor"],
            bordercolor=t["legend"]["bordercolor"],
            borderwidth=1,
            font=dict(size=12),
        ),
        margin=dict(l=60, r=40, t=60, b=60),
    )
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        linecolor=t["grid_color"],
        tickfont=dict(size=12, color=t["font_color"]),
        title_font=dict(size=13, color=t["font_color"]),
    )
    fig.update_yaxes(
        gridcolor=t["grid_color"],
        gridwidth=1,
        zeroline=False,
        linecolor="rgba(0,0,0,0)",
        tickfont=dict(size=12, color=t["font_color"]),
        title_font=dict(size=13, color=t["font_color"]),
    )
    return fig


# ── Private helpers ───────────────────────────────────────────────────────────


def _hour_label(h: int) -> str:
    """Convert 0–23 integer to readable AM/PM label."""
    if h == 0:
        return "12 AM"
    elif h < 12:
        return f"{h} AM"
    elif h == 12:
        return "12 PM"
    else:
        return f"{h - 12} PM"


# ── Chart 1 ───────────────────────────────────────────────────────────────────


def chart_trips_by_day_type(df: pd.DataFrame) -> Figure:
    """Grouped bar chart: trips by day type (weekday / weekend), split by customer type."""
    t = _get_theme()

    labels = {
        "day_type": "Day Type",
        "trip_count": "Total Trips",
        "customer_type": "Customer Type",
    }

    fig = px.bar(
        df,
        x="day_type",
        y="trip_count",
        labels=labels,
        color="customer_type",
        barmode="group",
        color_discrete_map=t["customer_colors"],
    )

    fig.update_traces(marker_line_width=0, opacity=0.92)

    _apply_theme(fig, title="Trips by Day Type")

    fig.update_xaxes(
        categoryorder="array",
        categoryarray=["weekday", "weekend"],
        ticktext=["Weekday", "Weekend"],
        tickvals=["weekday", "weekend"],
    )

    return fig


# ── Chart 2 ───────────────────────────────────────────────────────────────────


def chart_trips_by_hour(df: pd.DataFrame) -> Figure:
    """Stacked bar chart: total trips per hour of day, split by customer type."""
    t = _get_theme()

    df = (
        df[["trip_hour", "trip_count", "customer_type"]]
        .groupby(["trip_hour", "customer_type"], as_index=False)
        .sum()
    )

    labels = {
        "trip_hour": "Hour of Day",
        "trip_count": "Total Trips",
        "customer_type": "Customer Type",
    }

    fig = px.bar(
        df,
        x="trip_hour",
        y="trip_count",
        labels=labels,
        color="customer_type",
        barmode="stack",
        color_discrete_map=t["customer_colors"],
    )

    fig.update_traces(marker_line_width=0, opacity=0.92)

    _apply_theme(fig, title="Trips by Hour of Day")

    hour_vals = list(range(24))
    hour_texts = [_hour_label(h) for h in hour_vals]

    fig.update_xaxes(
        tickmode="array",
        tickvals=hour_vals,
        ticktext=hour_texts,
        tickangle=45,
        title_text="Hour of Day",
    )
    fig.update_yaxes(title_text="Total Trips")

    return fig


# ── Chart 3 ───────────────────────────────────────────────────────────────────


def chart_trips_by_month(df: pd.DataFrame) -> Figure:
    """Line chart: total trips per month, split by customer type."""
    t = _get_theme()

    df = (
        df[["year", "month", "trip_count", "customer_type"]]
        .groupby(["year", "month", "customer_type"], as_index=False)
        .sum()
    )
    df["year_month"] = (
        df["year"].astype(str) + "-" + df["month"].astype(str).str.zfill(2)
    )

    labels = {
        "trip_count": "Total Trips",
        "customer_type": "Customer Type",
        "year_month": "Month",
    }

    fig = px.line(
        df,
        x="year_month",
        y="trip_count",
        labels=labels,
        color="customer_type",
        color_discrete_map=t["customer_colors"],
        markers=True,
    )

    fig.update_traces(
        line=dict(width=2.5),
        marker=dict(size=7, line=dict(width=1.5, color=t["bg_color"])),
        opacity=0.95,
    )

    _apply_theme(fig, title="Monthly Trip Trends")

    fig.update_xaxes(
        tickangle=45,
        title_text="Month",
    )
    fig.update_yaxes(title_text="Total Trips")

    return fig


# ── Chart 4 ───────────────────────────────────────────────────────────────────


def chart_station_activity_start(df: pd.DataFrame) -> Figure:
    """Horizontal bar chart: top 10 start stations by trip count."""
    t = _get_theme()

    df = (
        df[df["station_role"] == "start"]
        .sort_values("trip_count", ascending=True)
        .tail(10)
    )

    labels = {
        "trip_count": "Total Trips",
        "station_name": "",
    }

    fig = px.bar(
        df,
        x="trip_count",
        y="station_name",
        labels=labels,
        color="station_role",
        color_discrete_map=t["station_colors"],
        orientation="h",
    )

    fig.update_traces(marker_line_width=0, opacity=0.92, width=0.6)

    _apply_theme(fig, title="Top Start Stations")

    fig.update_layout(
        showlegend=False,
        margin=dict(l=180, r=40, t=60, b=50),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=t["grid_color"],
        gridwidth=1,
        title_text="Total Trips",
    )
    fig.update_yaxes(
        showgrid=False,
        automargin=True,
        tickfont=dict(size=11),
    )

    return fig


# ── Chart 5 ───────────────────────────────────────────────────────────────────


def chart_station_activity_end(df: pd.DataFrame) -> Figure:
    """Horizontal bar chart: top 10 end stations by trip count."""
    t = _get_theme()

    df = (
        df[df["station_role"] == "end"]
        .sort_values("trip_count", ascending=True)
        .tail(10)
    )

    labels = {
        "trip_count": "Total Trips",
        "station_name": "",
    }

    fig = px.bar(
        df,
        x="trip_count",
        y="station_name",
        labels=labels,
        color="station_role",
        color_discrete_map=t["station_colors"],
        orientation="h",
    )

    fig.update_traces(marker_line_width=0, opacity=0.92, width=0.6)

    _apply_theme(fig, title="Top End Stations")

    fig.update_layout(
        showlegend=False,
        margin=dict(l=180, r=40, t=60, b=50),
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=t["grid_color"],
        gridwidth=1,
        title_text="Total Trips",
    )
    fig.update_yaxes(
        showgrid=False,
        automargin=True,
        tickfont=dict(size=11),
    )

    return fig


# ── Chart 6 ───────────────────────────────────────────────────────────────────

_METRIC_LABELS = {
    "avg_trip_duration_minutes": "Average",
    "median_trip_duration_minutes": "Median",
}

_RIDEABLE_LABELS = {
    "classic_bike": "Classic Bike",
    "electric_bike": "Electric Bike",
}

_CUSTOMER_LABELS = {
    "member": "Member",
    "casual": "Casual",
}


def chart_avg_duration_by_customer(df: pd.DataFrame) -> Figure:
    """Faceted grouped bar: avg & median trip duration by customer type and bike type."""
    t = _get_theme()

    df = df.copy()
    df = df.melt(
        id_vars=["customer_type", "rideable_type"],
        value_vars=["avg_trip_duration_minutes", "median_trip_duration_minutes"],
        var_name="metric",
        value_name="value",
    )

    df["metric"] = df["metric"].map(_METRIC_LABELS)
    df["rideable_type"] = df["rideable_type"].map(_RIDEABLE_LABELS)
    df["customer_type"] = df["customer_type"].map(_CUSTOMER_LABELS)

    labels = {
        "value": "Minutes",
        "customer_type": "Customer Type",
        "metric": "Metric",
    }

    fig = px.bar(
        df,
        x="customer_type",
        y="value",
        labels=labels,
        barmode="group",
        facet_col="rideable_type",
        color="metric",
        color_discrete_map=t["duration_colors"],
    )

    fig.update_traces(marker_line_width=0, opacity=0.92, width=0.35)

    _apply_theme(fig, title="Average Trip Duration by Customer & Bike Type")

    # Clean facet labels — strip "rideable_type=" prefix
    fig.for_each_annotation(
        lambda a: a.update(
            text=a.text.split("=")[-1],
            font=dict(size=13, color=t["font_color"]),
        )
    )

    # Single shared x-axis label
    fig.for_each_xaxis(lambda ax: ax.update(title_text=""))
    fig.add_annotation(
        text="Customer Type",
        xref="paper",
        yref="paper",
        x=0.5,
        y=-0.10,
        showarrow=False,
        font=dict(size=13, color=t["font_color"]),
    )

    # Y-axis title on left facet only
    fig.update_yaxes(title_text="")
    fig.layout.yaxis.title.text = "Minutes"

    return fig
