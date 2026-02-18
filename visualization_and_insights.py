# visualization_and_insights.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import networkx as nx
from config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_USER

# ------------------- DB Connection -------------------- #
engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# ------------------- Load Data ------------------------ #
print("Loading data from database...")
movies = pd.read_sql("SELECT * FROM movies", engine)
genres = pd.read_sql("SELECT * FROM genres", engine)
movie_genres = pd.read_sql("SELECT * FROM movie_genres", engine)
# FIX: Add quotes around reserved keyword 'cast'
cast = pd.read_sql('SELECT * FROM "cast"', engine)
crew = pd.read_sql("SELECT * FROM crew", engine)

# Directory to save images
import os
os.makedirs("visuals", exist_ok=True)

print("\nData Loaded Successfully!")
print(f"Movies: {len(movies)}, Cast: {len(cast)}, Crew: {len(crew)}")

# ====================== 5 BASIC VISUALIZATIONS ====================== #

print("\nGenerating basic visualizations...")

# 1. Popularity Distribution
plt.figure(figsize=(7,5))
sns.histplot(movies['popularity'].dropna(), bins=40)
plt.title("Movie Popularity Distribution")
plt.xlabel("Popularity")
plt.ylabel("Count")
plt.savefig("visuals/1_popularity_distribution.png", dpi=150, bbox_inches='tight')
plt.close()
print("1. Popularity distribution saved")

# 2. Vote Average Distribution
plt.figure(figsize=(7,5))
sns.histplot(movies['vote_average'].dropna(), bins=30, color='green')
plt.title("Vote Average Distribution")
plt.xlabel("Rating")
plt.ylabel("Count")
plt.savefig("visuals/2_rating_distribution.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 2. Rating distribution saved")

# 3. Top 15 genres by movie count
genre_count = movie_genres['genre_id'].value_counts().head(15)
genre_labels = genres.set_index("genre_id").loc[genre_count.index]['genre_name']

plt.figure(figsize=(10,5))
sns.barplot(x=genre_labels, y=genre_count.values, palette='viridis')
plt.xticks(rotation=45, ha='right')
plt.title("Top Genres by Movie Count")
plt.xlabel("Genre")
plt.ylabel("Number of Movies")
plt.savefig("visuals/3_top_genres.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 3. Top genres saved")

# 4. Movie releases by year
movies['year'] = pd.to_datetime(movies['release_date'], errors='coerce').dt.year
yearly = movies['year'].value_counts().sort_index()

plt.figure(figsize=(12,5))
sns.lineplot(x=yearly.index, y=yearly.values, linewidth=2)
plt.title("Movies Released Per Year")
plt.xlabel("Year")
plt.ylabel("Count")
plt.grid(True, alpha=0.3)
plt.savefig("visuals/4_movies_per_year.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 4. Movies per year saved")

# 5. Top 20 movies by revenue
top_rev = movies[movies['revenue'] > 0].sort_values("revenue", ascending=False).head(20)

plt.figure(figsize=(12,6))
sns.barplot(y=top_rev['title'], x=top_rev['revenue'], palette='rocket')
plt.title("Top 20 Highest Grossing Movies")
plt.xlabel("Revenue ($)")
plt.ylabel("Movie")
plt.savefig("visuals/5_top_revenue_movies.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 5. Top revenue movies saved")

print("\nBasic Visualizations ✔ Saved to visuals/")

# ====================== 5 ADVANCED INSIGHT VISUALIZATIONS ====================== #

print("\nGenerating advanced visualizations...")

# 6. Budget vs Revenue correlation
movies_valid = movies[(movies['budget'] > 0) & (movies['revenue'] > 0)]

plt.figure(figsize=(8,6))
sns.scatterplot(data=movies_valid, x="budget", y="revenue", alpha=0.6)
plt.title("Budget vs Revenue")
plt.xlabel("Budget ($)")
plt.ylabel("Revenue ($)")
# Add diagonal line for reference
max_val = max(movies_valid['budget'].max(), movies_valid['revenue'].max())
plt.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Break-even line')
plt.legend()
plt.savefig("visuals/6_budget_vs_revenue_scatter.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 6. Budget vs Revenue scatter saved")

# 7. Profitability analysis (ROI)
movies['profit'] = movies['revenue'] - movies['budget']
movies['roi'] = ((movies['profit'] / movies['budget']) * 100).replace([float('inf'), -float('inf')], 0)

# Filter for valid ROI (budget > 0, finite ROI)
movies_roi = movies[(movies['budget'] > 0) & (movies['roi'].notna()) & (movies['roi'] != 0)]
top_roi = movies_roi.sort_values("roi", ascending=False).head(20)

plt.figure(figsize=(12,7))
sns.barplot(x=top_roi['roi'], y=top_roi['title'], palette='coolwarm')
plt.title("Top 20 Most Profitable Movies (ROI %)")
plt.xlabel("Return on Investment (%)")
plt.ylabel("Movie")
plt.savefig("visuals/7_top_profit_movies.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 7. Top ROI movies saved")

# 8. Ratings vs Popularity Heatmap
df_corr = movies[['vote_average','vote_count','popularity','revenue','budget']].corr()

plt.figure(figsize=(8,6))
sns.heatmap(df_corr, annot=True, cmap="coolwarm", center=0, 
            square=True, linewidths=1, fmt='.2f')
plt.title("Correlation Heatmap - Movie Metrics")
plt.savefig("visuals/8_correlation_heatmap.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 8. Correlation heatmap saved")

# 9. Most Frequent Directors
directors = crew[crew['job'] == "Director"]
top_dir = directors['name'].value_counts().head(15)

plt.figure(figsize=(10,6))
sns.barplot(x=top_dir.values, y=top_dir.index, palette='mako')
plt.title("Top 15 Most Active Directors")
plt.xlabel("Number of Movies Directed")
plt.ylabel("Director")
plt.savefig("visuals/9_top_directors.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 9. Top directors saved")

# 10. Actor Collaboration Network (Graph)
print("Building actor collaboration network (this may take a moment)...")

# Get top 50 most active actors
top_cast_movies = cast.groupby("actor_name").movie_id.count().sort_values(ascending=False).head(50).index
collab = cast[cast['actor_name'].isin(top_cast_movies)]

# Build collaboration graph
G = nx.Graph()
for movie_id, group in collab.groupby("movie_id"):
    actors = list(group['actor_name'])
    for i in range(len(actors)):
        for j in range(i+1, len(actors)):
            if G.has_edge(actors[i], actors[j]):
                G[actors[i]][actors[j]]['weight'] += 1
            else:
                G.add_edge(actors[i], actors[j], weight=1)

# Draw network
plt.figure(figsize=(15,15))
pos = nx.spring_layout(G, k=0.5, iterations=50, seed=42)

# Draw edges with varying thickness based on collaboration frequency
edges = G.edges()
weights = [G[u][v]['weight'] for u,v in edges]

nx.draw_networkx_edges(G, pos, alpha=0.2, width=weights, edge_color='gray')
nx.draw_networkx_nodes(G, pos, node_size=100, node_color='lightblue', 
                        edgecolors='black', linewidths=1)
nx.draw_networkx_labels(G, pos, font_size=6, font_weight='bold')

plt.title("Actor Collaboration Network (Top 50 Actors)\nEdge thickness = Number of collaborations", 
          fontsize=14)
plt.axis('off')
plt.tight_layout()
plt.savefig("visuals/10_actor_network.png", dpi=150, bbox_inches='tight')
plt.close()
print(" 10. Actor network saved")

print("\nAdvanced Visualizations ✔ Saved to visuals/")

# ====================== SUMMARY STATISTICS ====================== #

print("\n" + "="*60)
print("VISUALIZATION SUMMARY")
print("="*60)

print(f"\nDataset Statistics:")
print(f"  • Total Movies: {len(movies):,}")
print(f"  • Total Cast Members: {cast['actor_name'].nunique():,}")
print(f"  • Total Crew Members: {crew['name'].nunique():,}")
print(f"  • Total Genres: {len(genres)}")
print(f"  • Year Range: {int(movies['year'].min())} - {int(movies['year'].max())}")

print(f"\nFinancial Stats:")
total_budget = movies['budget'].sum()
total_revenue = movies['revenue'].sum()
print(f"  • Total Budget: ${total_budget:,.0f}")
print(f"  • Total Revenue: ${total_revenue:,.0f}")
print(f"  • Total Profit: ${(total_revenue - total_budget):,.0f}")

print(f"\nRating Stats:")
print(f"  • Average Rating: {movies['vote_average'].mean():.2f}")
print(f"  • Highest Rated: {movies.loc[movies['vote_average'].idxmax(), 'title']} ({movies['vote_average'].max():.1f})")
print(f"  • Most Popular: {movies.loc[movies['popularity'].idxmax(), 'title']} ({movies['popularity'].max():.1f})")

print(f"\nTop 3 Genres:")
top_3_genres = genre_count.head(3)
for idx, (genre_id, count) in enumerate(top_3_genres.items(), 1):
    genre_name = genres.loc[genres['genre_id'] == genre_id, 'genre_name'].values[0]
    print(f"  {idx}. {genre_name}: {count} movies")

print(f"\nTop 3 Directors:")
for idx, (director, count) in enumerate(top_dir.head(3).items(), 1):
    print(f"  {idx}. {director}: {count} movies")

print(f"\nTop 3 Most Connected Actors:")
degree_centrality = nx.degree_centrality(G)
top_actors = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:3]
for idx, (actor, centrality) in enumerate(top_actors, 1):
    num_connections = G.degree(actor)
    print(f"  {idx}. {actor}: {num_connections} collaborations")

print("\n" + "="*60)
print("ALL 10 VISUALIZATIONS COMPLETE 🎉")
print("Location: visuals/ folder")
print("="*60)