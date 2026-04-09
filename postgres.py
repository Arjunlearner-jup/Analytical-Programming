import time
from typing import Dict, Tuple

import numpy as np
import pandas as pd
import requests
from dagster import Definitions, MetadataValue, Output, asset
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from requests.exceptions import ConnectionError, ReadTimeout
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2 import sql

COUNTRY = "IRL"
START_PERIOD, END_PERIOD = "2020-01", "2025-12"
FRED_API_KEY = "3582a4e99cf0c07405db1767fef8bedd"

INDICATORS = {
    "cli_amplitude_adjusted": {
        "source": "fred",
        "series_id": "IRLLOLITOAASTSAM",
        "title": "Leading Indicators OECD: CLI: Amplitude adjusted for Ireland",
        "frequency": "monthly",
        "units": "Index, Seasonally Adjusted",
    }
}

EMPTY_THRESHOLD = 0.95
ERROR_CODES = {-999, -99, -9}

PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASSWORD = "arjun"
PG_DB = "oecd_demo"

PG_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

MONGO_URL = "mongodb://localhost:27017"
MONGO_DB = "oecd_analytics"


def create_database_if_not_exists(
    db_name: str,
    user: str = "postgres",
    password: str = "arjun",
    host: str = "localhost",
    port: int = 5432,
) -> None:
    conn = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port,
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
    exists = cur.fetchone() is not None

    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database {db_name} created")
    else:
        print(f"Database {db_name} already exists")

    cur.close()
    conn.close()


create_database_if_not_exists(
    PG_DB,
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT,
)

engine = create_engine(PG_URL, pool_pre_ping=True)

mongo_client = None
if MONGO_URL:
    try:
        mongo_client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=10000)
        mongo_client.admin.command("ping")
        print("MongoDB connection successful")
    except ServerSelectionTimeoutError as e:
        print(f"MongoDB connection timeout: {e}")
        mongo_client = None
    except Exception as e:
        print(f"MongoDB connection error: {type(e).__name__}: {e}")
        mongo_client = None


def fetch_fred_series(indicator_meta: dict, max_retries: int = 3, sleep_seconds: int = 2) -> dict:
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": indicator_meta["series_id"],
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": f"{START_PERIOD}-01",
        "observation_end": f"{END_PERIOD}-31",
    }

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=60)
            r.raise_for_status()
            return r.json()
        except (ReadTimeout, ConnectionError) as e:
            if attempt == max_retries:
                raise RuntimeError(f"FRED fetch failed after {max_retries} attempts: {e}")
            time.sleep(sleep_seconds)
        except requests.HTTPError as e:
            raise RuntimeError(f"FRED HTTP error: {e} | Response: {r.text[:500]}")


def log_data_quality(df: pd.DataFrame, dataset_name: str, stage: str) -> Dict:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    dup_cols = ["date"] if "date" in df.columns else df.columns.tolist()

    total_cells = len(df) * len(df.columns)
    missing_cells = int(df.isnull().sum().sum())
    missing_pct = round((missing_cells / total_cells) * 100, 2) if total_cells else 0.0

    return {
        "dataset": dataset_name,
        "stage": stage,
        "total_rows": int(len(df)),
        "total_columns": int(len(df.columns)),
        "numeric_columns": int(len(numeric_cols)),
        "missing_cells": missing_cells,
        "missing_percentage": missing_pct,
        "duplicate_rows": int(df[dup_cols].duplicated().sum()) if len(df) else 0,
    }


def remove_empty_columns(df: pd.DataFrame, threshold: float = EMPTY_THRESHOLD) -> Tuple[pd.DataFrame, list]:
    if df.empty:
        return df, []

    missing_pct = df.isnull().mean()
    cols_to_drop = missing_pct[missing_pct > threshold].index.tolist()

    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    return df, cols_to_drop


def simple_imputation(df: pd.DataFrame) -> pd.DataFrame:
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if not numeric_cols:
        return df

    missing_counts = df[numeric_cols].isnull().sum()
    cols_with_missing = missing_counts[missing_counts > 0].index.tolist()
    if not cols_with_missing:
        return df

    df = df.copy()
    df[numeric_cols] = df[numeric_cols].ffill().bfill()

    remaining_nulls = df[numeric_cols].isnull().sum()
    cols_still_missing = remaining_nulls[remaining_nulls > 0].index.tolist()

    for col in cols_still_missing:
        df[col] = df[col].fillna(df[col].mean())

    return df


def fred_data_to_df(payload: dict, indicator_name: str, country: str = COUNTRY) -> pd.DataFrame:
    observations = payload.get("observations", [])
    if not observations:
        raise ValueError(f"No observations for {indicator_name}")

    df = pd.DataFrame(observations)
    required_cols = ["date", "value"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {indicator_name}: {missing_cols}")

    df = df[["date", "value"]].copy()
    df["country"] = country

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    print(log_data_quality(df, indicator_name, "Raw"))

    df["value"] = df["value"].replace(".", np.nan)
    df["value"] = df["value"].where(~df["value"].isin(ERROR_CODES), np.nan)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["value"])

    before_dedup = len(df)
    df = df.drop_duplicates(subset=["country", "date"], keep="last")
    if len(df) < before_dedup:
        print(f"Dropped {before_dedup - len(df)} duplicates in {indicator_name}")

    df = df.rename(columns={"value": f"{indicator_name}_value"})

    df["year"] = df["date"].dt.year
    df["decade"] = (df["year"] // 10 * 10).astype(str) + "s"
    df["is_recent"] = (df["year"] >= 2020).astype(int)

    df, dropped_cols = remove_empty_columns(df)
    if dropped_cols:
        print(f"Dropped sparse columns in {indicator_name}: {dropped_cols}")

    df = simple_imputation(df)

    print(log_data_quality(df, indicator_name, "Clean"))
    return df


def ensure_table_and_write(df: pd.DataFrame, table_name: str) -> None:
    df.to_sql(table_name, engine, if_exists="replace", index=False, method="multi", chunksize=5000)

    with engine.begin() as conn:
        if "date" in df.columns:
            conn.execute(
                text(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_date ON "{table_name}" ("date")')
            )
        if "year" in df.columns:
            conn.execute(
                text(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON "{table_name}" ("year")')
            )
        if "country" in df.columns:
            conn.execute(
                text(f'CREATE INDEX IF NOT EXISTS idx_{table_name}_country ON "{table_name}" ("country")')
            )


def write_to_mongodb(df: pd.DataFrame, collection_name: str) -> bool:
    if not mongo_client:
        return False

    try:
        db = mongo_client[MONGO_DB]
        collection = db[collection_name]

        records = df.replace({pd.NaT: None, np.nan: None}).to_dict("records")
        for record in records:
            if "date" in record and record["date"] is not None:
                record["date"] = pd.Timestamp(record["date"]).isoformat()

        collection.drop()

        if records:
            collection.insert_many(records)
            collection.create_index("date")
            if "country" in df.columns:
                collection.create_index("country")
            if "year" in df.columns:
                collection.create_index("year")
            return True
        return False

    except Exception as e:
        print(f"Error writing to MongoDB '{collection_name}': {e}")
        return False


@asset
def ab_raw() -> dict:
    raw_data = {}
    for name, indicator in INDICATORS.items():
        payload = fetch_fred_series(indicator)
        raw_data[name] = payload
        print(f"Fetched {name}: {len(payload.get('observations', []))} rows")
    return raw_data


@asset
def b_tables(ab_raw: dict) -> dict:
    cleaned_tables = {}
    for name, payload in ab_raw.items():
        cleaned_tables[name] = fred_data_to_df(payload, name)
    return cleaned_tables


@asset
def postgres_load(b_tables: dict) -> Output[str]:
    pg_tables_written = []
    mongo_collections_written = []

    for name, df in b_tables.items():
        if df is None or df.empty:
            raise ValueError(f"{name} is empty")

        ensure_table_and_write(df, name)
        pg_tables_written.append(name)

        if write_to_mongodb(df, name):
            mongo_collections_written.append(name)

    total_records = int(sum(len(df) for df in b_tables.values()))

    metadata_text = f"PostgreSQL: {', '.join(pg_tables_written)}"
    if mongo_collections_written:
        metadata_text += f" | MongoDB: {', '.join(mongo_collections_written)}"

    return Output(
        value="ETL Complete",
        metadata={
            "postgres_tables": MetadataValue.json(pg_tables_written),
            "mongodb_collections": MetadataValue.json(mongo_collections_written),
            "table_names": MetadataValue.text(metadata_text),
            "total_records": MetadataValue.int(total_records),
            "postgres_database": MetadataValue.text(f"{PG_DB}@{PG_HOST}"),
            "mongodb_database": MetadataValue.text(MONGO_DB if mongo_client else "Not Connected"),
        },
    )


defs = Definitions(assets=[ab_raw, b_tables, postgres_load])