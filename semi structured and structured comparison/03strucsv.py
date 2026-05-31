import pandas as pd
from io import StringIO

file_path = "open-meteo-59.91N10.74E9m.csv"

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

meta_header = lines[0].strip().split(",")
meta_values = lines[1].strip().split(",")
meta = dict(zip(meta_header, meta_values))

latitude = float(meta["latitude"])
longitude = float(meta["longitude"])
elevation = float(meta["elevation"])

data_start = next(i for i, line in enumerate(lines) if line.strip().startswith("time,"))
data_text = "".join(lines[data_start:])
df = pd.read_csv(StringIO(data_text))

df.columns = df.columns.str.strip()

print(df.columns)

temp_col = next(col for col in df.columns if "temperature_2m" in col)
df = df.rename(columns={temp_col: "temperature_2m"})

df["date"] = df["time"].str.split("T").str[0]
df["time"] = df["time"].str.split("T").str[1]

df["latitude"] = latitude
df["longitude"] = longitude
df["elevation"] = elevation

df = df[["date", "time", "temperature_2m", "latitude", "longitude", "elevation"]]

print(df.head())
df.to_csv("open-meteo-59.91N10.74E9m_cleaned.csv", index=False)