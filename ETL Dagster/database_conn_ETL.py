from sqlalchemy import create_engine, text


PG_HOST = "127.0.0.1"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "arjun"
PG_DB = "oecd_demo"

PG_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

try:
    engine = create_engine(PG_URL)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version();"))
        print("PostgreSQL connection successful!")
        print(f"   Version: {result.fetchone()[0]}")
except Exception as e:
    print(f"Connection failed: {e}")
