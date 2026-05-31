import requests
from pymongo import MongoClient
from datetime import datetime, timezone

API_URL = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=52.52&longitude=13.41"
    "&hourly=temperature_2m"
)

def fetch_weather_data():
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    return response.json()

def store_in_mongodb(data):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["weather_etl"]
    collection = db["raw_forecasts"]

    # ✅ FIX 1: Extract the hourly arrays from the payload
    hourly_times = data["hourly"]["time"]               # List of 168 ISO timestamps
    hourly_temps = data["hourly"]["temperature_2m"]     # List of 168 temperature values

    # ✅ FIX 2: Build one document per hour (flattened rows like the SQL table)
    documents = []
    for forecast_time_str, temp in zip(hourly_times, hourly_temps):
        documents.append({
            "source": "open-meteo",
            "api_url": API_URL,
            "ingested_at": datetime.now(timezone.utc),   # ✅ FIX 3: UTC-aware datetime
            "forecast_time": datetime.fromisoformat(forecast_time_str),  # ✅ FIX 4: Parse ISO string to datetime
            "temperature_2m": temp,                      # ✅ FIX 5: Actual temperature value stored
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "elevation": data["elevation"],
            "timezone": data["timezone"],
        })

    # ✅ FIX 6: Use insert_many for bulk insert of all 168 hourly rows
    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} documents.")
    client.close()  # ✅ FIX 7: Always close the connection

if __name__ == "__main__":
    weather_data = fetch_weather_data()
    store_in_mongodb(weather_data)