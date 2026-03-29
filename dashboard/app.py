# Total Trips — from mart_trips_by_month
# Avg Trip Duration — from mart_avg_duration_by_customer
# Most Active Station — from mart_station_activity
# Member vs Casual split % — from mart_trips_by_month
# Busiest Hour — from mart_trips_by_hour

# These tell a story: how much usage, how long, where, who, and when.

import streamlit as st
from data import (
    get_trips_by_month,
    get_trips_by_hour,
    get_trips_by_day_type,
    get_station_activity,
    get_avg_duration_by_customer,
    get_kpis,
)
from charts import (
    chart_trips_by_month,
    chart_trips_by_hour,
    chart_trips_by_day_type,
    chart_station_activity_start,
    chart_station_activity_end,
    chart_avg_duration_by_customer,
)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Citibike Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────

st.markdown(
    """
    <style>
        /* Tighten default Streamlit top padding */
        .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

        /* KPI metric cards */
        [data-testid="metric-container"] {
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 10px;
            padding: 1rem 1.2rem;
        }
        [data-testid="stMetricLabel"] { font-size: 0.78rem; opacity: 0.65; }
        [data-testid="stMetricValue"] { font-size: 1.6rem; font-weight: 700; }

        /* Section dividers */
        .section-header {
            font-size: 1.05rem;
            font-weight: 600;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            opacity: 0.45;
            margin-top: 2rem;
            margin-bottom: 0.25rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ────────────────────────────────────────────────────────────────────

st.title("🚲 Citibike Analytics")
st.caption("NYC bike-share usage patterns · powered by BigQuery + dbt")

st.divider()

# ── KPI row ───────────────────────────────────────────────────────────────────

with st.spinner("Loading data…"):
    kpis = get_kpis()

col1, col2, col3, col4, col5 = st.columns(5)

col1.metric("Total Trips", f"{kpis['Total Trips']:,}")
col2.metric("Avg Trip Duration", f"{kpis['Avg Trip Duration']} min")
col3.metric("Member / Casual Split", kpis["Member vs Casual split %"] + " %")
col4.metric("Most Active Station", kpis["Most Active Station"])
col5.metric("Busiest Hour", kpis["Busiest Hour"] + ":00")

st.divider()

# ── Section 1: Overview ───────────────────────────────────────────────────────

st.markdown('<div class="section-header">Overview</div>', unsafe_allow_html=True)

col_left, col_right = st.columns([3, 2])

with col_left:
    df_month = get_trips_by_month()
    st.plotly_chart(
        chart_trips_by_month(df_month),
        use_container_width=True,
    )

with col_right:
    df_day_type = get_trips_by_day_type()
    st.plotly_chart(
        chart_trips_by_day_type(df_day_type),
        use_container_width=True,
    )

# ── Section 2: Operations ─────────────────────────────────────────────────────

st.markdown('<div class="section-header">Operations</div>', unsafe_allow_html=True)

df_hour = get_trips_by_hour()
st.plotly_chart(
    chart_trips_by_hour(df_hour),
    use_container_width=True,
)

col_start, col_end = st.columns(2)

df_stations = get_station_activity()

with col_start:
    st.plotly_chart(
        chart_station_activity_start(df_stations),
        use_container_width=True,
    )

with col_end:
    st.plotly_chart(
        chart_station_activity_end(df_stations),
        use_container_width=True,
    )

# ── Section 3: Customer Insights ─────────────────────────────────────────────

st.markdown(
    '<div class="section-header">Customer Insights</div>', unsafe_allow_html=True
)

df_duration = get_avg_duration_by_customer()
st.plotly_chart(
    chart_avg_duration_by_customer(df_duration),
    use_container_width=True,
)

st.caption(
    "Median shown alongside average due to outlier skew "
    "(e.g. casual classic bike: avg 24.9 min vs median 12.3 min)."
)
