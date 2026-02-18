#!/usr/bin/env python
# coding: utf-8

# In[2]:


import requests
import gzip
import pandas as pd
from sqlalchemy import create_engine, Text
from sqlalchemy.types import TEXT
import os
from config import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB

engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
    pool_size=10, max_overflow=20, execution_options={"isolation_level": "AUTOCOMMIT"}
)

urls = {
    "title.basics.tsv.gz":   "https://datasets.imdbws.com/title.basics.tsv.gz",
    "title.ratings.tsv.gz":  "https://datasets.imdbws.com/title.ratings.tsv.gz",
    "title.crew.tsv.gz":     "https://datasets.imdbws.com/title.crew.tsv.gz",
    "name.basics.tsv.gz":    "https://datasets.imdbws.com/name.basics.tsv.gz",
    "title.akas.tsv.gz":     "https://datasets.imdbws.com/title.akas.tsv.gz"
}

os.makedirs("data/imdb_raw", exist_ok=True)

# Reasonable chunk size – works perfectly on 8–16 GB laptops
CHUNK_SIZE = 100_000
MAX_RECORDS = 2000  # Limit per table

for filename, url in urls.items():
    local_path = f"data/imdb_raw/{filename}"
    print(f"\nDownloading {filename} ...")
    r = requests.get(url, stream=True, timeout=600)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    table_name = filename.replace(".tsv.gz", "").replace(".", "_")
    print(f"Streaming {filename} → table '{table_name}' in chunks of {CHUNK_SIZE:,} rows (max {MAX_RECORDS:,})")

    first_chunk = True
    total_rows = 0

    with gzip.open(local_path, "rb") as f:
        for chunk_df in pd.read_csv(f, sep="\t", chunksize=CHUNK_SIZE,
                                    low_memory=False, na_values="\\N", dtype=str):

            # Keep only movies for title.basics (huge reduction)
            if filename == "title.basics.tsv.gz":
                chunk_df = chunk_df[chunk_df["titleType"] == "movie"]

            # Skip if already at limit
            if total_rows >= MAX_RECORDS:
                print(f"\nHit {MAX_RECORDS:,} record limit for {table_name}")
                break

            rows_to_add = min(len(chunk_df), MAX_RECORDS - total_rows)
            if rows_to_add == 0:
                break

            chunk_subset = chunk_df.head(rows_to_add)

            # Create dtype dictionary for all columns as TEXT
            dtype_dict = {col: TEXT for col in chunk_subset.columns}

            chunk_subset.to_sql(
                name=table_name,
                con=engine,
                if_exists="append" if not first_chunk else "replace",
                index=False,
                method="multi",
                dtype=dtype_dict  # Use SQLAlchemy TEXT type
            )

            total_rows += rows_to_add
            print(f"   → {total_rows:,} rows loaded so far...", end="\r")
            first_chunk = False

    print(f"\nFinished {table_name} | Total rows = {total_rows:,}")
    # Truncate table to exact limit if slightly over (edge case)
    if total_rows > MAX_RECORDS:
        engine.execute(f"DELETE FROM {table_name} WHERE ctid NOT IN (SELECT ctid FROM {table_name} LIMIT {MAX_RECORDS})")

print("\nAll IMDb datasets loaded safely with chunking (limited to 2000 records each)!")


# In[2]:





# In[3]:





# In[4]:





# In[5]:





# In[8]:





# In[7]:




