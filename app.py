#!/usr/bin/env python
# coding: utf-8

import streamlit as st
import plotly.express as px
import pandas as pd
from pandas.api.types import is_numeric_dtype
from utils import get_db_connection, load_fact_table

# ----------------------------
# Page configuration & style
# ----------------------------

st.set_page_config(
    page_title="OECD Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main {background-color: #f5f7fa;}
    .stMetric {
        background-color: white;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stPlotlyChart {
        background-color: white;
        border-radius: 10px;
        padding: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1, h2, h3 {color: #1f4e79;}
</style>
""", unsafe_allow_html=True)

st.title("OECD Analytics Dashboard")
st.markdown("### Economic Indicators Analysis")
st.markdown("---")

# ----------------------------
# Load data from database
# ----------------------------

@st.cache_data(show_spinner=False)
def get_data():
    conn = get_db_connection()
    df = load_fact_table(conn)
    return df

try:
    df = get_data()

    if df is None or df.empty:
        st.error("No data returned from the database.")
        st.stop()

    df.columns = df.columns.astype(str).str.strip().str.lower()

except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.stop()

# ----------------------------
# Validate and standardize columns
# ----------------------------

column_aliases = {
    "time": ["time", "date", "datetime", "timestamp", "observation_date", "period"],
    "country": ["country", "country_name", "location", "nation"],
    "cli_amplitude_adjusted_value": [
        "cli_amplitude_adjusted_value",
        "cli", "indicator_value"
    ],
}

resolved_columns = {}

for standard_col, aliases in column_aliases.items():
    found = next((col for col in aliases if col in df.columns), None)
    if found:
        resolved_columns[standard_col] = found

missing_essential = [col for col in ["country"] if col not in resolved_columns]
if missing_essential:
    st.error(f"Missing essential columns: {missing_essential}")
    st.stop()

rename_map = {v: k for k, v in resolved_columns.items()}
df = df.rename(columns=rename_map)

# ----------------------------
# Create year column
# ----------------------------

if "time" in df.columns:
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"])
    df["year"] = df["time"].dt.year
elif "year" in df.columns:
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["year"])
else:
    st.error("No usable 'time' or 'year' column found in the dataset.")
    st.stop()

df["year"] = df["year"].astype(int)

# ----------------------------
# Ensure numeric columns are numeric
# ----------------------------

candidate_numeric = [
    col for col in df.columns
    if col not in ["time", "year", "country", "decade", "is_recent"]
]

for col in candidate_numeric:
    df[col] = pd.to_numeric(df[col], errors="coerce")

numeric_cols = [col for col in candidate_numeric if is_numeric_dtype(df[col])]

if not numeric_cols:
    st.error("No indicator columns available for analysis.")
    st.stop()

# ----------------------------
# Defaults
# ----------------------------

country_options = sorted(df["country"].dropna().unique().tolist())
year_min = int(df["year"].min())
year_max = int(df["year"].max())
available_indicator_options = [col for col in numeric_cols if col in df.columns]

if "selected_countries" not in st.session_state:
    st.session_state.selected_countries = country_options

if "year_range" not in st.session_state:
    st.session_state.year_range = (year_min, year_max)

if "selected_indicator" not in st.session_state:
    st.session_state.selected_indicator = available_indicator_options[0]

# ----------------------------
# Sidebar filters in form
# ----------------------------

st.sidebar.header("Filters")
st.sidebar.markdown("---")

with st.sidebar.form("filter_form"):
    selected_countries = st.multiselect(
        "Select Countries",
        options=country_options,
        default=st.session_state.selected_countries
    )

    year_range = st.slider(
        "Select Year Range",
        min_value=year_min,
        max_value=year_max,
        value=st.session_state.year_range,
        step=1
    )

    selected_indicator = st.selectbox(
        "Primary Indicator",
        options=available_indicator_options,
        index=available_indicator_options.index(st.session_state.selected_indicator)
        if st.session_state.selected_indicator in available_indicator_options else 0
    )

    submitted = st.form_submit_button("Apply Filters")

if submitted:
    st.session_state.selected_countries = selected_countries
    st.session_state.year_range = year_range
    st.session_state.selected_indicator = selected_indicator

selected_countries = st.session_state.selected_countries
year_range = st.session_state.year_range
selected_indicator = st.session_state.selected_indicator


# ----------------------------
# KPI metric cards
# ----------------------------

latest_year = filtered_df["year"].max()
latest_df = filtered_df[filtered_df["year"] == latest_year]

overall_avg = df[selected_indicator].mean() if df[selected_indicator].notna().any() else None
filtered_avg = latest_df[selected_indicator].mean() if latest_df[selected_indicator].notna().any() else None
total_records = len(filtered_df)
country_count = filtered_df["country"].nunique()
year_span = f"{year_range[0]}–{year_range[1]}"

col1, col2, col3, col4 = st.columns(4)

with col1:
    delta_val = None
    if filtered_avg is not None and overall_avg is not None:
        delta_val = f"{filtered_avg - overall_avg:.2f}"
    st.metric(
        label=f"Avg {selected_indicator.replace('_', ' ').title()}",
        value=f"{filtered_avg:.2f}" if filtered_avg is not None else "N/A",
        delta=delta_val
    )

with col2:
    st.metric(
        label="Countries Selected",
        value=country_count
    )

with col3:
    st.metric(
        label="Filtered Records",
        value=f"{total_records:,}"
    )

with col4:
    st.metric(
        label="Years in Range",
        value=year_span
    )

st.markdown("---")

# ----------------------------
# Interactive line chart
# ----------------------------

st.markdown("## Interactive Line Chart")

trend_df = (
    filtered_df.groupby(["year", "country"], as_index=False)[selected_indicator]
    .mean()
    .dropna()
)

if not trend_df.empty:
    fig_line = px.line(
        trend_df,
        x="year",
        y=selected_indicator,
        color="country",
        markers=True,
        title=f"{selected_indicator.replace('_', ' ').title()} Trend by Year",
        labels={
            "year": "Year",
            selected_indicator: selected_indicator.replace("_", " ").title(),
            "country": "Country"
        }
    )
    fig_line.update_layout(
        height=500,
        hovermode="x unified",
        plot_bgcolor="white",
        paper_bgcolor="white"
    )
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.info("No trend data available for the selected filters.")

# ----------------------------
# Summary statistics table
# ----------------------------

st.markdown("## Summary Statistics Table")

summary_df = filtered_df[[selected_indicator]].describe().T.reset_index()
summary_df = summary_df.rename(columns={"index": "Indicator"})

st.dataframe(
    summary_df.style.format({
        "count": "{:.0f}",
        "mean": "{:.2f}",
        "std": "{:.2f}",
        "min": "{:.2f}",
        "25%": "{:.2f}",
        "50%": "{:.2f}",
        "75%": "{:.2f}",
        "max": "{:.2f}",
    }),
    use_container_width=True
)

# ----------------------------
# Optional latest year comparison
# ----------------------------

st.markdown("## Latest Year Comparison")

latest_compare_df = latest_df[["country", selected_indicator]].dropna()

if not latest_compare_df.empty:
    fig_bar = px.bar(
        latest_compare_df.sort_values(selected_indicator, ascending=False),
        x="country",
        y=selected_indicator,
        color="country",
        title=f"{selected_indicator.replace('_', ' ').title()} by Country ({latest_year})"
    )
    fig_bar.update_layout(showlegend=False, height=500)
    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.info("No latest-year comparison data available.")