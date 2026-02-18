# integrate_tmdb_imdb.py

import pandas as pd
from sqlalchemy import create_engine
from config import *

engine = create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

print("Loading data...")

tmdb = pd.read_sql("SELECT movie_id,title,release_date,revenue,budget,vote_average,vote_count FROM movies", engine)
tmdb["year"] = pd.to_datetime(tmdb["release_date"], errors="coerce").dt.year.astype("Int64")

imdb_basic = pd.read_sql('SELECT tconst, "primaryTitle", "startYear" FROM title_basics', engine)
imdb_rating = pd.read_sql('SELECT tconst, "averageRating", "numVotes" FROM title_ratings', engine)

imdb = imdb_basic.merge(imdb_rating, on="tconst", how="left")

# FIX: Convert to numeric, coerce errors to NaN
imdb["startYear"] = pd.to_numeric(imdb["startYear"], errors="coerce")
imdb["averageRating"] = pd.to_numeric(imdb["averageRating"], errors="coerce")
imdb["numVotes"] = pd.to_numeric(imdb["numVotes"], errors="coerce")

print("Merging on title + year...")

merged = tmdb.merge(imdb,
                    left_on=["title", "year"],
                    right_on=["primaryTitle", "startYear"],
                    how="left")

final = merged[[
    "movie_id","title","release_date","year","budget","revenue",
    "vote_average","vote_count","averageRating","numVotes","tconst"
]].rename(columns={
    "vote_average": "tmdb_rating",
    "vote_count": "tmdb_votes",
    "averageRating": "imdb_rating",
    "numVotes": "imdb_votes"
})

# FIX: Ensure numeric types for all rating/vote columns
final["tmdb_rating"] = pd.to_numeric(final["tmdb_rating"], errors="coerce")
final["tmdb_votes"] = pd.to_numeric(final["tmdb_votes"], errors="coerce")
final["imdb_rating"] = pd.to_numeric(final["imdb_rating"], errors="coerce")
final["imdb_votes"] = pd.to_numeric(final["imdb_votes"], errors="coerce")
final["budget"] = pd.to_numeric(final["budget"], errors="coerce")
final["revenue"] = pd.to_numeric(final["revenue"], errors="coerce")

# Calculate profit and ROI
final["profit"] = final["revenue"] - final["budget"]
final["roi"] = ((final["revenue"] - final["budget"]) / final["budget"] * 100).round(2)

final.to_sql("movies_integrated", engine, if_exists="replace", index=False)
print(f"Integrated dataset created ✓ {len(final)} movies")
print(f"IMDb matched: {final['tconst'].notna().sum()} / {len(final)}")
print(f"Match rate: {final['tconst'].notna().mean():.1%}")