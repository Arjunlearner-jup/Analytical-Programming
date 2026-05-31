import pandas as pd

pg_df = pd.read_csv("raw_forecasts.csv")
csv_df = pd.read_csv("open-meteo-59.91N10.74E9m_cleaned.csv")

pg_df.columns = pg_df.columns.str.strip().str.lower()
csv_df.columns = csv_df.columns.str.strip().str.lower()

pg_df["date"] = pd.to_datetime(pg_df["forecast_date"]).dt.strftime("%Y-%m-%d")
pg_df["time"] = pd.to_datetime(
    pg_df["forecast_time_only"].astype(str),
    format="%H:%M:%S",
    errors="coerce"
).dt.strftime("%H:%M")

csv_df["date"] = pd.to_datetime(csv_df["date"]).dt.strftime("%Y-%m-%d")
csv_df["time"] = pd.to_datetime(
    csv_df["time"].astype(str),
    format="%H:%M",
    errors="coerce"
).dt.strftime("%H:%M")

pg_df = pg_df[[
    "date",
    "time",
    "temperature_2m",
    "latitude",
    "longitude",
    "elevation"
]].rename(columns={
    "temperature_2m": "temperature_2m_psql",
    "latitude": "latitude_psql",
    "longitude": "longitude_psql",
    "elevation": "elevation_psql"
})

csv_df = csv_df[[
    "date",
    "time",
    "temperature_2m",
    "latitude",
    "longitude",
    "elevation"
]].rename(columns={
    "temperature_2m": "temperature_2m_csv",
    "latitude": "latitude_csv",
    "longitude": "longitude_csv",
    "elevation": "elevation_csv"
})

# Add row IDs so both 72-row tables align row-by-row
pg_df = pg_df.reset_index(drop=True)
csv_df = csv_df.reset_index(drop=True)
pg_df["row_id"] = pg_df.index
csv_df["row_id"] = csv_df.index

final_df = pd.merge(
    pg_df,
    csv_df,
    on="row_id",
    how="inner",
    suffixes=("_psql", "_csv")
)

final_df = final_df[[
    "date_psql",
    "time_psql",
    "date_csv",
    "time_csv",
    "temperature_2m_psql",
    "temperature_2m_csv",
    "latitude_psql",
    "latitude_csv",
    "longitude_psql",
    "longitude_csv",
    "elevation_psql",
    "elevation_csv"
]]

final_df.to_csv("final_weather_comparison.csv", index=False)

print("Rows:", len(final_df))
print(final_df.head())