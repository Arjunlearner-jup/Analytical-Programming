import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Database connection with caching
PG_HOST = "127.0.0.1"
PG_PORT = "5432"
PG_USER = "postgres"
PG_PASSWORD = "arjun"
PG_DB = "oecd_demo"

@st.cache_resource
def get_db_connection():
    """Create cached db connection"""
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url)

@st.cache_data(ttl=600)
def load_table(_conn, table_name):
    """Load table from database with caching"""
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, _conn)
    return df

@st.cache_data(ttl=600)
def load_fact_table(_conn):
    df = load_table(_conn, "public.cli_amplitude_adjusted")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df = df.dropna(subset=["date", "year", "country", "cli_amplitude_adjusted_value"])
    df["year"] = df["year"].astype(int)
    return df
    
def apply_custom_theme(fig, title=""):
    """Apply professional styling to Plotly figures"""
    fig.update_layout(
        title=dict(text=title, font=dict(size=20, color="#1f77b4", family="Arial Black")),
        template="plotly_white",
        hovermode="x unified",
        font=dict(family="Arial", size=12),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(240,240,240,0.5)",
        margin=dict(l=50, r=50, t=80, b=50),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(255,255,255,0.8)"
        )
    )
    return fig

COLORS = {
    "primary": "#1f77b4",
    "secondary": "#ff7f0e",
    "success": "#2ca02c",
    "warning": "#d62728",
    "info": "#9467bd",
    "gradient": ["#0d47a1", "#1976d2", "#42a5f5", "#90caf9"]
}