#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from config import *


# In[2]:


headers = {"Authorization": f"Bearer {TMDB_BEARER}", "accept": "application/json"}


# In[3]:


# DB SETUP ---------------------------- #
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
raw_col = db[RAW_COLLECTION]
credits_col = db["credits_raw"]


# In[4]:


postgres_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(postgres_url)


# In[5]:


# STEP 4 — Fetch Detailed & Credits Data ----------------- #
def enrich_movies():
    movies = list(raw_col.find({}, {"movie_id": 1}))
    for i, m in enumerate(movies):
        movie_id = m["movie_id"]

        # details
        det = requests.get(f"https://api.themoviedb.org/3/movie/{movie_id}", headers=headers)
        if det.status_code == 200:
            raw_col.update_one({"movie_id": movie_id}, {"$set": {"details": det.json()}})

        # credits
        cr = requests.get(f"https://api.themoviedb.org/3/movie/{movie_id}/credits", headers=headers)
        if cr.status_code == 200:
            credits_col.update_one({"movie_id": movie_id}, {"$set": cr.json()}, upsert=True)

        print(f"Enriched {i+1}/{len(movies)}")
    print("Enrichment Complete!")


# In[6]:


# Clear all tables before inserting new data -------------- #
def clear_tables():
    """Drop all tables in correct order (respecting foreign keys)"""
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS movie_genres CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS \"cast\" CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS crew CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS movies CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS genres CASCADE"))
        conn.commit()
    print("Tables cleared!")


# In[7]:


# STEP 5 & 6 — Transform & Store in PostgreSQL ------------ #
def transform_and_store(max_records=2000):
    movies = raw_col.find({"details": {"$exists": True}}).limit(max_records)

    movie_data, cast_data, crew_data, genre_data, map_data = [], [], [], [], []
    processed_count = 0

    for m in movies:
        if processed_count >= max_records:
            break
        det = m.get("details", {})
        movie_id = det.get("id")
        if not movie_id:
            continue

        movie_data.append({
            "movie_id": movie_id,
            "title": det.get("title"),
            "overview": det.get("overview"),
            "release_date": det.get("release_date"),
            "popularity": det.get("popularity"),
            "vote_average": det.get("vote_average"),
            "vote_count": det.get("vote_count"),
            "runtime": det.get("runtime"),
            "budget": det.get("budget"),
            "revenue": det.get("revenue"),
            "status": det.get("status"),
            "language": det.get("original_language"),
        })

        # genres
        for g in det.get("genres", []):
            genre_data.append({"genre_id": g["id"], "genre_name": g["name"]})
            map_data.append({"movie_id": movie_id, "genre_id": g["id"]})

        # credits
        credits = credits_col.find_one({"movie_id": movie_id})
        if credits:
            for c in credits.get("cast", []):
                cast_data.append({
                    "movie_id": movie_id, "actor_id": c["id"], "actor_name": c["name"],
                    "character_name": c.get("character"), "gender": c.get("gender"), 
                    "popularity": c.get("popularity")
                })
            for c in credits.get("crew", []):
                crew_data.append({
                    "movie_id": movie_id, "person_id": c["id"], "name": c["name"],
                    "job": c.get("job"), "department": c.get("department")
                })

        processed_count += 1

    # Convert & Insert into PostgreSQL ----------------------- #
    pd.DataFrame(movie_data).drop_duplicates(subset="movie_id").to_sql(
        "movies", engine, if_exists="replace", index=False
    )
    pd.DataFrame(genre_data).drop_duplicates(subset="genre_id").to_sql(
        "genres", engine, if_exists="replace", index=False
    )
    pd.DataFrame(map_data).to_sql(
        "movie_genres", engine, if_exists="replace", index=False
    )
    pd.DataFrame(cast_data).to_sql(
        "cast", engine, if_exists="replace", index=False
    )
    pd.DataFrame(crew_data).to_sql(
        "crew", engine, if_exists="replace", index=False
    )

    print(f"Data transformed & loaded into PostgreSQL successfully! Processed {processed_count} movies.")


# In[8]:


if __name__ == "__main__":
    enrich_movies()
    clear_tables()  # Clear tables before inserting
    transform_and_store()


# In[ ]:




