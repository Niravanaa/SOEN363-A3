DROP TABLE IF EXISTS rating_providers CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;

-- Table to store rating providers (e.g., Internet Movie Database, Rotten Tomatoes, Metacritic)
CREATE TABLE rating_providers (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Table to store movie ratings
CREATE TABLE ratings (
    id SERIAL PRIMARY KEY,
    film_id SERIAL NOT NULL,
    imdb_id TEXT NOT NULL,  -- IMDb ID for the movie
    rating_provider_id INT NOT NULL,
    rating_value TEXT NOT NULL,  -- Ratings may be in different formats (e.g., "8.4/10", "94%", "78/100")
    FOREIGN KEY (rating_provider_id) REFERENCES rating_providers(id) ON DELETE CASCADE,
    FOREIGN KEY (film_id) REFERENCES films(id) ON DELETE CASCADE
);
