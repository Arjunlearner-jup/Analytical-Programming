from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd
from config import *

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
raw = db[RAW_COLLECTION]

engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

# record counts
raw_count = raw.count_documents({})
movies_sql = pd.read_sql("SELECT COUNT(*) FROM movies", engine).iloc[0,0]
cast_sql = pd.read_sql("SELECT COUNT(*) FROM cast", engine).iloc[0,0]
crew_sql = pd.read_sql("SELECT COUNT(*) FROM crew", engine).iloc[0,0]

print(f"Raw MongoDB movies: {raw_count}")
print(f"Movies in Postgres:  {movies_sql}")
print(f"Cast Count:           {cast_sql}")
print(f"Crew Count:           {crew_sql}")

# dedup check
dupes = pd.read_sql("""
    SELECT movie_id, COUNT(*) 
    FROM movies 
    GROUP BY movie_id 
    HAVING COUNT(*) > 1;
""", engine)

print("\nDuplicates found:" if not dupes.empty else "\nNo duplicates ✔")
print(dupes)
