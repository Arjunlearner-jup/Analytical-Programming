-- schema.sql -- Run once using pgAdmin / DBeaver / psql
CREATE TABLE IF NOT EXISTS movies (
    movie_id INT PRIMARY KEY,
    title TEXT,
    overview TEXT,
    release_date DATE,
    popularity FLOAT,
    vote_average FLOAT,
    vote_count INT,
    runtime INT,
    budget BIGINT,
    revenue BIGINT,
    status TEXT,
    language TEXT
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id INT PRIMARY KEY,
    genre_name TEXT
);

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id INT,
    genre_id INT,
    FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
    FOREIGN KEY(genre_id) REFERENCES genres(genre_id)
);

CREATE TABLE IF NOT EXISTS "cast" (
    movie_id INT,
    actor_id INT,
    actor_name TEXT,
    character_name TEXT,
    gender INT,
    popularity FLOAT
);

CREATE TABLE IF NOT EXISTS crew (
    movie_id INT,
    person_id INT,
    name TEXT,
    job TEXT,
    department TEXT
);

CREATE INDEX IF NOT EXISTS idx_movie_id ON movies(movie_id);
CREATE INDEX IF NOT EXISTS idx_genre_id ON genres(genre_id);
CREATE INDEX IF NOT EXISTS idx_cast_movie ON "cast"(movie_id);
CREATE INDEX IF NOT EXISTS idx_crew_movie ON crew(movie_id);