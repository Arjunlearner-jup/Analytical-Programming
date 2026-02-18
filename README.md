# TMDB + IMDb Movie Analytics Project

## Datasets
1. TMDB 5000+ movies (API – semi-structured JSON) – collected by Arjun Mohan
2. IMDb title.basics, title.ratings, title.crew, name.basics, title.akas (TSV – structured) – collected by Arjun Mohan

## Run order
1. python collect_store_raw.py
2. python ETL.py
3. python imdb_structured.py
4. python integrate_tmdb_imdb.py
5. python validation.py
6. python visualization_and_insights.py
7. streamlit run dashboard.py

## Setup
- MongoDB running on localhost:27017
- PostgreSQL database tmdb_analytics with user postgres
- config.py containing bearer code, api key and postgres password


All data processing is 100% programmatic – no manual steps.
