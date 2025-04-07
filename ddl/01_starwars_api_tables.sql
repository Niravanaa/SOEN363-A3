-- Drop tables if they exist
DROP TABLE IF EXISTS people_films CASCADE;
DROP TABLE IF EXISTS people_species CASCADE;
DROP TABLE IF EXISTS people_starships CASCADE;
DROP TABLE IF EXISTS people_vehicles CASCADE;
DROP TABLE IF EXISTS films_species CASCADE;
DROP TABLE IF EXISTS films_starships CASCADE;
DROP TABLE IF EXISTS films_vehicles CASCADE;
DROP TABLE IF EXISTS films_planets CASCADE;

DROP TABLE IF EXISTS people CASCADE;
DROP TABLE IF EXISTS films CASCADE;
DROP TABLE IF EXISTS starships CASCADE;
DROP TABLE IF EXISTS vehicles CASCADE;
DROP TABLE IF EXISTS species CASCADE;
DROP TABLE IF EXISTS planets CASCADE;

-- Create tables
CREATE TABLE people (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    birth_year VARCHAR(20),
    eye_color VARCHAR(50),
    gender VARCHAR(20),
    hair_color VARCHAR(50),
    height VARCHAR(20),
    mass VARCHAR(20),
    skin_color VARCHAR(50),
    homeworld VARCHAR(255),
    url VARCHAR(255) UNIQUE NOT NULL,
    created TIMESTAMP,
    edited TIMESTAMP
);

CREATE TABLE films (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    episode_id INTEGER NOT NULL,
    opening_crawl TEXT,
    director VARCHAR(255),
    producer VARCHAR(255),
    release_date DATE,
    url VARCHAR(255) UNIQUE NOT NULL,
    created TIMESTAMP,
    edited TIMESTAMP
);

CREATE TABLE starships (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    model VARCHAR(255),
    starship_class VARCHAR(255),
    manufacturer VARCHAR(255),
    cost_in_credits VARCHAR(50),
    length VARCHAR(50),
    crew VARCHAR(50),
    passengers VARCHAR(50),
    max_atmosphering_speed VARCHAR(50),
    hyperdrive_rating VARCHAR(50),
    MGLT VARCHAR(50),
    cargo_capacity VARCHAR(50),
    consumables VARCHAR(255),
    url VARCHAR(255) UNIQUE NOT NULL,
    created TIMESTAMP,
    edited TIMESTAMP
);

CREATE TABLE vehicles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    model VARCHAR(255),
    vehicle_class VARCHAR(255),
    manufacturer VARCHAR(255),
    length VARCHAR(50),
    cost_in_credits VARCHAR(50),
    crew VARCHAR(50),
    passengers VARCHAR(50),
    max_atmosphering_speed VARCHAR(50),
    cargo_capacity VARCHAR(50),
    consumables VARCHAR(255),
    url VARCHAR(255) UNIQUE NOT NULL,
    created TIMESTAMP,
    edited TIMESTAMP
);

CREATE TABLE species (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    classification VARCHAR(255),
    designation VARCHAR(50),
    average_height VARCHAR(50),
    average_lifespan VARCHAR(50),
    eye_colors VARCHAR(255),
    hair_colors VARCHAR(255),
    skin_colors VARCHAR(255),
    language VARCHAR(255),
    homeworld VARCHAR(255),
    url VARCHAR(255) UNIQUE NOT NULL,
    created TIMESTAMP,
    edited TIMESTAMP
);

CREATE TABLE planets (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    diameter VARCHAR(50),
    rotation_period VARCHAR(50),
    orbital_period VARCHAR(50),
    gravity VARCHAR(50),
    population VARCHAR(50),
    climate VARCHAR(255),
    terrain VARCHAR(255),
    surface_water VARCHAR(50),
    url VARCHAR(255) UNIQUE NOT NULL,
    created TIMESTAMP,
    edited TIMESTAMP
);

-- Relationship Tables for Many-to-Many Relationships
CREATE TABLE people_films (
    person_url VARCHAR(255) REFERENCES people(url) ON DELETE CASCADE,
    film_url VARCHAR(255) REFERENCES films(url) ON DELETE CASCADE,
    PRIMARY KEY (person_url, film_url)
);

CREATE TABLE people_species (
    person_url VARCHAR(255) REFERENCES people(url) ON DELETE CASCADE,
    species_url VARCHAR(255) REFERENCES species(url) ON DELETE CASCADE,
    PRIMARY KEY (person_url, species_url)
);

CREATE TABLE people_starships (
    person_url VARCHAR(255) REFERENCES people(url) ON DELETE CASCADE,
    starship_url VARCHAR(255) REFERENCES starships(url) ON DELETE CASCADE,
    PRIMARY KEY (person_url, starship_url)
);

CREATE TABLE people_vehicles (
    person_url VARCHAR(255) REFERENCES people(url) ON DELETE CASCADE,
    vehicle_url VARCHAR(255) REFERENCES vehicles(url) ON DELETE CASCADE,
    PRIMARY KEY (person_url, vehicle_url)
);

CREATE TABLE films_species (
    film_url VARCHAR(255) REFERENCES films(url) ON DELETE CASCADE,
    species_url VARCHAR(255) REFERENCES species(url) ON DELETE CASCADE,
    PRIMARY KEY (film_url, species_url)
);

CREATE TABLE films_starships (
    film_url VARCHAR(255) REFERENCES films(url) ON DELETE CASCADE,
    starship_url VARCHAR(255) REFERENCES starships(url) ON DELETE CASCADE,
    PRIMARY KEY (film_url, starship_url)
);

CREATE TABLE films_vehicles (
    film_url VARCHAR(255) REFERENCES films(url) ON DELETE CASCADE,
    vehicle_url VARCHAR(255) REFERENCES vehicles(url) ON DELETE CASCADE,
    PRIMARY KEY (film_url, vehicle_url)
);

CREATE TABLE films_planets (
    film_url VARCHAR(255) REFERENCES films(url) ON DELETE CASCADE,
    planet_url VARCHAR(255) REFERENCES planets(url) ON DELETE CASCADE,
    PRIMARY KEY (film_url, planet_url)
);