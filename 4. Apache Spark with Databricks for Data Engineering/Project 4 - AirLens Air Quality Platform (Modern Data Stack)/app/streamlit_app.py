"""AirLens dashboard backed by the local read-only DuckDB serving file."""

from pathlib import Path

import duckdb
import pandas as pd
import pydeck as pdk
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "serving" / "airlens.duckdb"

SCALE_LABELS = {
    "openweather-1-5": "OpenWeather AQI, 1 to 5",
    "us-epa-0-500": "WAQI US EPA AQI, 0 to 500",
}

POLLUTANTS = {
    "PM2.5 (ug/m3)": "pm25_ugm3",
    "PM10 (ug/m3)": "pm10_ugm3",
    "O3 (ug/m3)": "o3_ugm3",
    "NO2 (ug/m3)": "no2_ugm3",
}


@st.cache_data
def load_data(database_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load serving tables, closing the read-only connection immediately."""
    connection = duckdb.connect(database_path, read_only=True)
    try:
        air = connection.execute(
            "select * from city_air_quality order by measured_at_utc"
        ).fetchdf()
        stations = connection.execute(
            "select * from stations order by query_city, station_name"
        ).fetchdf()
    finally:
        connection.close()
    return air, stations


def aqi_color(value: float, scale: str) -> list[int]:
    """Return a map color without comparing values across AQI scales."""
    if scale == "openweather-1-5":
        palette = {
            1: [42, 157, 143, 190],
            2: [233, 196, 106, 190],
            3: [244, 162, 97, 190],
            4: [231, 111, 81, 190],
            5: [157, 78, 221, 190],
        }
        return palette.get(int(round(value)), [110, 110, 110, 180])
    if value <= 50:
        return [42, 157, 143, 190]
    if value <= 100:
        return [233, 196, 106, 190]
    if value <= 150:
        return [244, 162, 97, 190]
    if value <= 200:
        return [231, 111, 81, 190]
    if value <= 300:
        return [157, 78, 221, 190]
    return [128, 0, 38, 210]


st.set_page_config(page_title="AirLens", page_icon="🌍", layout="wide")
st.title("AirLens global air quality")
st.caption(
    "AQI values are never averaged or compared across scales. OpenWeather uses 1 to 5, "
    "while WAQI reports the US EPA 0 to 500 scale."
)

if not DATABASE_PATH.exists():
    st.error("The serving database is missing. Run `python serving/build_duckdb.py` first.")
    st.stop()

air, stations = load_data(str(DATABASE_PATH))

map_scale = st.selectbox(
    "Map AQI scale",
    options=list(SCALE_LABELS),
    format_func=SCALE_LABELS.get,
)
map_data = (
    air.loc[air["aqi_scale"] == map_scale]
    .sort_values("measured_at_utc")
    .groupby("city", as_index=False)
    .tail(1)
    .copy()
)
map_data["color"] = map_data.apply(
    lambda row: aqi_color(float(row["aqi_value"]), row["aqi_scale"]), axis=1
)
map_data["radius"] = 70000

layer = pdk.Layer(
    "ScatterplotLayer",
    data=map_data,
    get_position="[lon, lat]",
    get_fill_color="color",
    get_radius="radius",
    radius_min_pixels=7,
    radius_max_pixels=45,
    stroked=True,
    get_line_color=[255, 255, 255],
    line_width_min_pixels=1,
    opacity=0.85,
    pickable=True,
)
view = pdk.ViewState(latitude=20, longitude=20, zoom=1.1)
st.pydeck_chart(
    pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        # Carto basemap: free and token-free, unlike the Mapbox default which
        # renders an empty canvas unless you supply an API token.
        map_provider="carto",
        map_style="light",
        tooltip={
            "html": "<b>{city}</b><br/>AQI: {aqi_value}<br/>Scale: {aqi_scale}",
            "style": {"backgroundColor": "#111827", "color": "white"},
        },
    ),
    height=420,
    width="stretch",
)
st.caption(f"Map scale: {SCALE_LABELS[map_scale]}. Values from another scale are hidden.")

left, right = st.columns(2)
with left:
    st.subheader("Per-city pollutant trend")
    selected_city = st.selectbox("City", sorted(air["city"].unique()))
    pollutant_label = st.selectbox("Pollutant", list(POLLUTANTS))
    pollutant_column = POLLUTANTS[pollutant_label]
    trend = air.loc[
        (air["city"] == selected_city) & air[pollutant_column].notna(),
        ["measured_at_utc", pollutant_column],
    ]
    if trend.empty:
        st.info("No concentration measurement is available for this city and pollutant.")
    else:
        st.line_chart(trend, x="measured_at_utc", y=pollutant_column)
    st.caption("Pollutant charts use OpenWeather concentrations in ug/m3, not WAQI sub-indexes.")

with right:
    st.subheader("City comparison")
    comparison_scale = st.selectbox(
        "Comparison AQI scale",
        options=list(SCALE_LABELS),
        format_func=SCALE_LABELS.get,
    )
    comparison = (
        air.loc[air["aqi_scale"] == comparison_scale]
        .sort_values("measured_at_utc")
        .groupby("city", as_index=False)
        .tail(1)
        .sort_values("aqi_value", ascending=False)
    )
    st.bar_chart(comparison, x="city", y="aqi_value")
    st.caption(f"Comparison scale: {SCALE_LABELS[comparison_scale]}.")

st.subheader("PM2.5 alerts")
threshold = st.slider("Alert threshold (ug/m3)", min_value=0, max_value=250, value=35)
alerts = (
    air.loc[air["pm25_ugm3"].notna() & (air["pm25_ugm3"] > threshold)]
    .sort_values("pm25_ugm3", ascending=False)
    [["city", "pm25_ugm3", "pm25_category", "measured_at_utc"]]
)
if alerts.empty:
    st.success(f"No cities exceed {threshold} ug/m3 in this snapshot.")
else:
    st.dataframe(alerts, width="stretch", hide_index=True)

st.caption(
    f"Serving file: {DATABASE_PATH.name}. Station registry rows available: {len(stations)}. "
    "PM2.5 categories are descriptive bands because this project has snapshots, not a regulatory 24-hour average."
)
