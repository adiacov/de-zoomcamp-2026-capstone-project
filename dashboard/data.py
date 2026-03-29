import streamlit as st
import pandas as pd

from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()


client = bigquery.Client()


def _make_mart_table_name(table_name: str) -> str:
    """Returns table name in the citibike marts"""
    project = client.project
    return f"`{project}.de_citibike_marts.{table_name}`"


@st.cache_data
def get_trips_by_month() -> pd.DataFrame:
    table = _make_mart_table_name("mart_trips_by_month")
    query = f"""
        SELECT
            year, month, customer_type, rideable_type, trip_count
        FROM {table}
    """

    return client.query(query=query).to_dataframe()


@st.cache_data
def get_trips_by_hour() -> pd.DataFrame:
    table = _make_mart_table_name("mart_trips_by_hour")
    query = f"""
        SELECT
            trip_date, trip_hour, customer_type, rideable_type, trip_count
        FROM {table}
    """
    return client.query(query=query).to_dataframe()


@st.cache_data
def get_trips_by_day_type() -> pd.DataFrame:
    table = _make_mart_table_name("mart_trips_by_day_type")
    query = f"""
        SELECT
            day_type, customer_type, rideable_type, trip_count
        FROM {table}
    """
    return client.query(query=query).to_dataframe()


@st.cache_data
def get_station_activity() -> pd.DataFrame:
    table = _make_mart_table_name("mart_station_activity")
    query = f"""
        SELECT
            station_name, station_role, trip_count
        FROM {table}
    """
    return client.query(query=query).to_dataframe()


@st.cache_data
def get_avg_duration_by_customer() -> pd.DataFrame:
    table = _make_mart_table_name("mart_avg_duration_by_customer")
    query = f"""
        SELECT
            customer_type, rideable_type, avg_trip_duration_minutes, median_trip_duration_minutes
        FROM {table}
    """
    return client.query(query=query).to_dataframe()


def get_kpis() -> dict:
    trips = get_trips_by_month()
    total_trips = int(trips["trip_count"].sum())

    aggregated = trips.groupby("customer_type")["trip_count"].sum()
    member_pct = (aggregated.get("member", 0) / total_trips * 100).round(2)
    casual_pct = (aggregated.get("casual", 0) / total_trips * 100).round(2)
    split_pct = "0/0" if total_trips == 0 else f"{member_pct}/{casual_pct}"

    durations = get_avg_duration_by_customer()
    avg_trip_duration = float(durations["avg_trip_duration_minutes"].mean().round(2))

    st_activity = get_station_activity()
    active_station = (
        st_activity[st_activity["station_role"] == "start"]
        .sort_values("trip_count", ascending=False)
        .iloc[0]
    )
    kpi_active_station = active_station["station_name"]

    hour_trips = get_trips_by_hour()
    hour_agg = hour_trips.groupby("trip_hour")["trip_count"].sum()
    kpi_busiest_hour = str(hour_agg.idxmax())

    return {
        "Total Trips": total_trips,
        "Member vs Casual split %": split_pct,
        "Avg Trip Duration": avg_trip_duration,
        "Most Active Station": kpi_active_station,
        "Busiest Hour": kpi_busiest_hour,
    }
