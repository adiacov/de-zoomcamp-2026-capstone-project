import dashboard as st

# Total Trips — from mart_trips_by_month
# Avg Trip Duration — from mart_avg_duration_by_customer
# Most Active Station — from mart_station_activity
# Member vs Casual split % — from mart_trips_by_month
# Busiest Hour — from mart_trips_by_hour

# These tell a story: how much usage, how long, where, who, and when.

st.title("CitiBike Trips Dashboard")

st.header("KPI Cards")
st.text("Total Trips | Avg Duration | Active Stations")

st.text("Trips Over Time (line chart)")

st.text("Top Stations | Bottom Stations (bar charts)")

st.text("User Type | Bike Type | Heatmap (distribution)")

st.text("Map (station activity)")
