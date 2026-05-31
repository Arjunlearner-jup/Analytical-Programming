from pymongo import MongoClient
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch
import pandas as pd

POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "arjun"
TARGET_DB = "weather_123"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "weather_etl"
MONGO_COLLECTION = "raw_forecasts"


def create_database_if_not_exists():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname="postgres",
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TARGET_DB,))
    exists = cur.fetchone()

    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TARGET_DB)))
        print(f"Database '{TARGET_DB}' created.")
    else:
        print(f"Database '{TARGET_DB}' already exists.")

    cur.close()
    conn.close()


def load_mongo_to_postgres_and_save_csv():
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[MONGO_COLLECTION]

    pg_conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=TARGET_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    pg_cursor = pg_conn.cursor()

    try:
        # Create table with unique forecast_time so duplicates are prevented
        pg_cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_forecasts (
            id SERIAL PRIMARY KEY,
            source TEXT,
            api_url TEXT,
            ingested_at TIMESTAMPTZ,
            forecast_time TIMESTAMPTZ UNIQUE,
            temperature_2m DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            elevation DOUBLE PRECISION,
            timezone TEXT,
            forecast_date DATE,
            forecast_time_only TIME
        )
        """)
        pg_conn.commit()

        total_docs = mongo_collection.count_documents({})
        print(f"Total MongoDB documents found: {total_docs}")

        mongo_docs = list(
            mongo_collection
            .find({}, {"_id": 0})
            .sort("forecast_time", 1)
            .limit(72)
        )

        print(f"Documents fetched from MongoDB: {len(mongo_docs)}")

        rows = [
            (
                doc.get("source"),
                doc.get("api_url"),
                doc.get("ingested_at"),
                doc.get("forecast_time"),
                doc.get("temperature_2m"),
                doc.get("latitude"),
                doc.get("longitude"),
                doc.get("elevation"),
                doc.get("timezone"),
                doc.get("forecast_time").date() if doc.get("forecast_time") else None,
                doc.get("forecast_time").time() if doc.get("forecast_time") else None,
            )
            for doc in mongo_docs
        ]

        if rows:
            insert_sql = """
            INSERT INTO raw_forecasts (
                source,
                api_url,
                ingested_at,
                forecast_time,
                temperature_2m,
                latitude,
                longitude,
                elevation,
                timezone,
                forecast_date,
                forecast_time_only
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (forecast_time) DO NOTHING
            """

            execute_batch(pg_cursor, insert_sql, rows)
            pg_conn.commit()

            pg_cursor.execute("SELECT COUNT(*) FROM raw_forecasts")
            total_rows = pg_cursor.fetchone()[0]
            print(f"Processed {len(rows)} rows from MongoDB.")
            print(f"Total rows currently in PostgreSQL: {total_rows}")

            # Save PostgreSQL table to CSV
            export_query = """
            SELECT
                source,
                api_url,
                ingested_at,
                forecast_time,
                temperature_2m,
                latitude,
                longitude,
                elevation,
                timezone,
                forecast_date,
                forecast_time_only
            FROM raw_forecasts
            ORDER BY forecast_time
            """
            export_df = pd.read_sql_query(export_query, pg_conn)
            export_df.to_csv("raw_forecasts.csv", index=False)
            print("Saved PostgreSQL table to raw_forecasts.csv")
        else:
            print("No rows found in MongoDB to insert.")

    except Exception as e:
        pg_conn.rollback()
        print("Error:", e)

    finally:
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    create_database_if_not_exists()
    load_mongo_to_postgres_and_save_csv()