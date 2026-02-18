# dashboard.py

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from config import *

# Page config
st.set_page_config(page_title="Movie Analytics Dashboard", layout="wide")
st.title("🎬 TMDb + IMDb Movie Analytics")

# Database connection
@st.cache_resource
def get_engine():
    return create_engine(f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

engine = get_engine()

# Load data
@st.cache_data
def load_data():
    df = pd.read_sql("SELECT * FROM movies_integrated", engine)
    
    # Ensure all numeric columns are proper numeric types
    numeric_cols = ['budget', 'revenue', 'tmdb_rating', 'tmdb_votes', 'imdb_rating', 'imdb_votes', 'profit', 'roi']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

movies = load_data()

# Sidebar filters
st.sidebar.header("Filters")

# Get valid year range (excluding NaN)
valid_years = movies['year'].dropna()
year_min = int(valid_years.min())
year_max = int(valid_years.max())

year_range = st.sidebar.slider(
    "Release Year",
    year_min,
    year_max,
    (2000, year_max)
)

# Get valid vote range
valid_votes = movies['tmdb_votes'].dropna()
votes_max = int(valid_votes.max()) if len(valid_votes) > 0 else 10000

min_votes = st.sidebar.slider(
    "Minimum TMDb Votes",
    0,
    votes_max,
    1000
)

# Filter data
filtered_movies = movies[
    (movies['year'] >= year_range[0]) &
    (movies['year'] <= year_range[1]) &
    (movies['tmdb_votes'] >= min_votes)
].copy()

# Metrics
st.header("📊 Overview")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Movies", f"{len(filtered_movies):,}")

with col2:
    # FIX: Use skipna=True to ignore NaN values
    avg_tmdb = filtered_movies['tmdb_rating'].mean(skipna=True)
    st.metric("Avg TMDb Rating", f"{avg_tmdb:.2f}" if pd.notna(avg_tmdb) else "N/A")

with col3:
    # FIX: Use skipna=True to ignore NaN values
    avg_imdb = filtered_movies['imdb_rating'].mean(skipna=True)
    st.metric("Avg IMDb Rating", f"{avg_imdb:.2f}" if pd.notna(avg_imdb) else "N/A")

with col4:
    match_rate = (filtered_movies['tconst'].notna().sum() / len(filtered_movies)) * 100
    st.metric("IMDb Match Rate", f"{match_rate:.1f}%")

# Charts
st.header("📈 Visualizations")

col1, col2 = st.columns(2)

with col1:
    # TMDb Rating Distribution
    tmdb_ratings = filtered_movies['tmdb_rating'].dropna()
    fig1 = px.histogram(
        tmdb_ratings, 
        x=tmdb_ratings.values, 
        nbins=20,
        title="TMDb Rating Distribution",
        labels={'x': 'TMDb Rating'}
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    # IMDb Rating Distribution
    imdb_ratings = filtered_movies['imdb_rating'].dropna()
    if len(imdb_ratings) > 0:
        fig2 = px.histogram(
            imdb_ratings,
            x=imdb_ratings.values,
            nbins=20,
            title="IMDb Rating Distribution",
            labels={'x': 'IMDb Rating'}
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No IMDb ratings available for the selected filters")

# Revenue vs Budget
st.subheader("💰 Revenue vs Budget")
revenue_data = filtered_movies[
    (filtered_movies['revenue'] > 0) & 
    (filtered_movies['budget'] > 0) &
    (filtered_movies['tmdb_rating'].notna())
].copy()

if len(revenue_data) > 0:
    fig_revenue = px.scatter(
        revenue_data,
        x='budget',
        y='revenue',
        color='tmdb_rating',
        hover_data=['title', 'year'],
        title='Revenue vs Budget (colored by TMDb rating)',
        labels={'budget': 'Budget ($)', 'revenue': 'Revenue ($)', 'tmdb_rating': 'TMDb Rating'}
    )
    st.plotly_chart(fig_revenue, use_container_width=True)
else:
    st.info("No revenue/budget data available for the selected filters")

# Top movies by ROI
st.subheader("🏆 Top 10 Movies by ROI")
roi_data = filtered_movies[filtered_movies['roi'].notna()].copy()
if len(roi_data) > 0:
    top_roi = roi_data.nlargest(10, 'roi')[
        ['title', 'year', 'budget', 'revenue', 'roi', 'tmdb_rating', 'imdb_rating']
    ]
    st.dataframe(top_roi, use_container_width=True)
else:
    st.info("No ROI data available for the selected filters")

# Rating comparison
st.subheader("⭐ TMDb vs IMDb Ratings")
matched_movies = filtered_movies[
    (filtered_movies['imdb_rating'].notna()) &
    (filtered_movies['tmdb_rating'].notna())
].copy()

if len(matched_movies) > 0:
    fig_ratings = px.scatter(
        matched_movies,
        x='tmdb_rating',
        y='imdb_rating',
        hover_data=['title', 'year'],
        title='TMDb vs IMDb Rating Comparison',
        labels={'tmdb_rating': 'TMDb Rating', 'imdb_rating': 'IMDb Rating'}
    )
    # Add diagonal line (perfect correlation)
    fig_ratings.add_shape(
        type='line',
        x0=0, y0=0, x1=10, y1=10,
        line=dict(color='red', dash='dash')
    )
    st.plotly_chart(fig_ratings, use_container_width=True)
else:
    st.info("No matched ratings available for the selected filters")

# Movies over time
st.subheader("📅 Movies Released Over Time")
movies_per_year = filtered_movies.groupby('year').size().reset_index(name='count')
if len(movies_per_year) > 0:
    fig_timeline = px.line(
        movies_per_year,
        x='year',
        y='count',
        title='Number of Movies Released by Year',
        labels={'year': 'Year', 'count': 'Number of Movies'}
    )
    st.plotly_chart(fig_timeline, use_container_width=True)

# Data table
st.subheader("🔍 Explore Data")
display_cols = ['title', 'year', 'budget', 'revenue', 'profit', 'roi', 'tmdb_rating', 'imdb_rating']
st.dataframe(
    filtered_movies[display_cols].sort_values('year', ascending=False),
    use_container_width=True
)