---
output:
  html_document: default
  pdf_document: default
---
# Agricultural Price Indices Dashboard

A Streamlit dashboard that explores the **effect of the Iraq War (2003–2011)** on agricultural input and output price indices using a JSON dataset hosted on GitHub.

## Overview

- Downloads JSON data from a GitHub raw URL.
- Loads the data into a pandas DataFrame.
- Extracts the year from the `Month` field.
- Filters records for the Iraq War period from **2003 to 2011**.
- Lets users filter by agricultural product and year range.
- Displays the results as either a line chart or a clustered bar chart.

## Features

- Interactive Streamlit sidebar filters
- Product selection with multiselect
- Year range filter with slider
- Choice of line chart or clustered bar chart
- Clean chart rendering with Matplotlib

## Technologies Used

- Python
- Streamlit
- pandas
- requests
- Matplotlib
- GitHub raw JSON as the data source

## Code Explanation

### 1. Import libraries

```python
import requests
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
```

These libraries are used for:
- `requests`: downloading JSON data from the web
- `pandas`: loading and transforming the dataset
- `matplotlib`: plotting charts
- `streamlit`: creating the dashboard UI

### 2. Configure the Streamlit page

```python
st.set_page_config(page_title="Agricultural Price Indices", layout="wide")
```

This sets the browser tab title and uses a wide layout for better chart visibility.

### 3. Add title and caption

```python
st.title("Effect of Iraq war(03-11) on agricultural Input and Output Price Indices")
st.caption("Filter by year and agricultural product to create readable charts.")
```

These lines create the main heading and a short app description.

### 4. Load data from GitHub

```python
url = "https://raw.githubusercontent.com/Arjunlearner-jup/uploading-json/refs/heads/main/AHM02.json"
data = requests.get(url, timeout=60).json()
```

The app fetches a JSON file from GitHub and converts it into Python data.

### 5. Convert JSON to DataFrame

```python
df = pd.DataFrame(data)
df["Year"] = df["Month"].str[:4].astype(int)
```

- `pd.DataFrame(data)` turns the JSON data into tabular form.
- A new `Year` column is created by extracting the first four characters from the `Month` column.

### 6. Filter the required years

```python
df_filtered = df.loc[
    (df["Year"] >= 2003) & (df["Year"] <= 2011),
    ["Year", "Agricultural Product", "VALUE"]
].reset_index(drop=True)
```

This keeps only the data between **2003 and 2011** and selects the relevant columns:
- `Year`
- `Agricultural Product`
- `VALUE`

### 7. Build the product list

```python
products = sorted(df_filtered["Agricultural Product"].unique())
```

This collects the unique agricultural products and sorts them for use in the sidebar filter.

### 8. Sidebar filters

```python
st.sidebar.header("Filters")
selected_products = st.sidebar.multiselect(
    "Select Agricultural Product",
    products,
    default=products[:5]
)
```

This creates a multiselect widget so users can choose which products to display.

```python
year_range = st.sidebar.slider(
    "Select Year Range",
    min_value=int(df_filtered["Year"].min()),
    max_value=int(df_filtered["Year"].max()),
    value=(2003, 2011)
)
```

This adds a slider to choose the year range.

```python
chart_type = st.sidebar.radio(
    "Chart Type",
    ["Line Chart", "Clustered Bar Chart"]
)
```

This allows the user to switch between two chart types.

### 9. Apply user-selected filters

```python
filtered = df_filtered[
    (df_filtered["Year"] >= year_range[0]) &
    (df_filtered["Year"] <= year_range[1]) &
    (df_filtered["Agricultural Product"].isin(selected_products))
]
```

This narrows the dataset based on the chosen year range and selected products.

### 10. Create the pivot table

```python
df_pivot = filtered.pivot_table(
    index="Year",
    columns="Agricultural Product",
    values="VALUE",
    aggfunc="mean"
)
```

This reshapes the data so:
- each row is a year
- each column is an agricultural product
- each value is the average index for that year and product

### 11. Plot the chart

```python
fig, ax = plt.subplots(figsize=(12, 6))
```

This creates the chart canvas.

For the line chart:

```python
df_pivot.plot(kind="line", marker="o", linewidth=2, ax=ax)
```

For the clustered bar chart:

```python
df_pivot.plot(kind="bar", ax=ax)
```

The chart is then labeled and formatted:

```python
ax.set_xlabel("Year")
ax.set_ylabel("Index (Base 2005=100)")
ax.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
```

Finally, the chart is displayed in Streamlit:

```python
st.pyplot(fig)
```

## No database is used

Data is directly used from the pandas dataframe