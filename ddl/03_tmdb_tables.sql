-- Delete the movie_metadata table if it exists
DROP TABLE IF EXISTS movie_metadata;

-- Table to store additional movie metadata (popularity and keywords)
CREATE TABLE movie_metadata (
    id INT PRIMARY KEY,
    imdb_id TEXT UNIQUE NOT NULL,
    popularity NUMERIC NOT NULL,
    overview VARCHAR(1000),
    runtime INTEGER,
    keywords TEXT NOT NULL,
    FOREIGN KEY (id) REFERENCES films(id) ON DELETE CASCADE
);
