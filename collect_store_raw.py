import requests
import time
from pymongo import MongoClient
from config import TMDB_API_KEY, TMDB_BEARER, MONGO_URI, MONGO_DB, RAW_COLLECTION

# ---------------- Connect to TMDB API ---------------- #
headers = {
    "Authorization": f"Bearer {TMDB_BEARER}",
    "accept": "application/json"
}

# ---------------- MongoDB Connection ---------------- #
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]
raw_col = db[RAW_COLLECTION]


def fetch_movies(category="popular", pages=150):
    base_url = f"https://api.themoviedb.org/3/movie/{category}?page="
    all_movies = []

    for page in range(1, pages+1):
        url = base_url + str(page)
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed Page {page}")
            continue

        data = response.json().get("results", [])
        print(f"Fetched page {page}, movies={len(data)}")

        for movie in data:
            movie_id = movie["id"]
            raw_col.update_one(
                {"movie_id": movie_id},
                {"$set": {"movie_id": movie_id, "raw": movie}},
                upsert=True
            )

        time.sleep(0.2)   # avoid rate limiting

    print("Raw Movie Collection Completed!")


if __name__ == "__main__":
    fetch_movies(category="top_rated", pages=150) # or popular
