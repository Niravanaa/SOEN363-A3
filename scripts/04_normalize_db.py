import psycopg2

from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

starwars_db_conn = psycopg2.connect(
    dbname=os.getenv("UNNORMALIZED_DB_NAME"),
    user= os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

normalized_db_conn = psycopg2.connect(
    dbname=os.getenv("NORMALIZED_DB_NAME"),
    user= os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

SCHEMA_FILE = "ddl/04_final_schema.sql"

try:
    cursor = normalized_db_conn.cursor()

    # Read the SQL schema file
    with open(SCHEMA_FILE, "r", encoding="utf-8") as file:
        sql_script = file.read()

    # Execute the SQL script
    cursor.execute(sql_script)
    normalized_db_conn.commit()

    print("Normalized Schema executed successfully.")

except Exception as e:
    print(f"Error executing schema: {e}")

# Create a cursor for both databases
starwars_db_cursor = starwars_db_conn.cursor()
normalized_db_cursor = normalized_db_conn.cursor()

# ============================================================
# PHASE 1: Migrating 'films' Data from starwars_db to normalized_starwars_db
# ============================================================

# Query to get data from the 'films' table and the 'popularity' from 'movie_metadata' in starwars_db
starwars_db_cursor.execute("""
    SELECT 
        f.id, f.title, f.episode_id, f.opening_crawl, f.director, f.producer, 
        f.release_date, f.url, f.created, f.edited, m.popularity, m.imdb_id
    FROM films f
    LEFT JOIN movie_metadata m ON f.id = m.id
""")

# Fetch all rows from the query result
films = starwars_db_cursor.fetchall()

# Insert data into the 'films' table in normalized_starwars_db
for film in films:
    id, title, episode_id, opening_crawl, director, producer, release_date, url, created, edited, popularity, imdb_id = film
    
    # Prepare the SQL statement to insert into the normalized films table
    insert_query = """
        INSERT INTO films (id, title, episode_id, opening_crawl, director, producer, 
                           release_date, url, created, edited, popularity, imdb_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Execute the insertion in the normalized_starwars_db
    normalized_db_cursor.execute(insert_query, (id, title, episode_id, opening_crawl, director, producer, 
                                                release_date, url, created, edited, popularity, imdb_id))

# Commit the transaction and close the connections
normalized_db_conn.commit()

# ============================================================
# End of PHASE 1
# ============================================================

# ============================================================
# PHASE 2: Migrating 'people', 'starships', 'vehicles', 'species', and 'planets' Data from starwars_db to normalized_starwars_db
# ============================================================

# People Table
starwars_db_cursor.execute("SELECT id, name, birth_year, eye_color, gender, hair_color, height, mass, skin_color, homeworld, url, created, edited FROM people")
people = starwars_db_cursor.fetchall()
for person in people:
    id, name, birth_year, eye_color, gender, hair_color, height, mass, skin_color, homeworld, url, created, edited = person
    insert_query = """
        INSERT INTO people (id, name, birth_year, eye_color, gender, hair_color, height, mass, skin_color, homeworld, url, created, edited)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    normalized_db_cursor.execute(insert_query, (id, name, birth_year, eye_color, gender, hair_color, height, mass, skin_color, homeworld, url, created, edited))

# Starships Table
starwars_db_cursor.execute("SELECT id, name, model, starship_class, manufacturer, cost_in_credits, length, crew, passengers, max_atmosphering_speed, hyperdrive_rating, MGLT, cargo_capacity, consumables, url, created, edited FROM starships")
starships = starwars_db_cursor.fetchall()
for starship in starships:
    # Unpack the data into corresponding variables
    id, name, model, starship_class, manufacturer, cost_in_credits, length, crew, passengers, max_atmosphering_speed, hyperdrive_rating, MGLT, cargo_capacity, consumables, url, created, edited = starship
    
    # Prepare the SQL statement to insert into the normalized starships table
    insert_query = """
        INSERT INTO starships (id, name, model, starship_class, manufacturer, cost_in_credits, length, crew, passengers, max_atmosphering_speed, hyperdrive_rating, MGLT, cargo_capacity, consumables, url, created, edited)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Execute the insertion in the normalized_starwars_db
    normalized_db_cursor.execute(insert_query, (id, name, model, starship_class, manufacturer, cost_in_credits, length, crew, passengers, max_atmosphering_speed, hyperdrive_rating, MGLT, cargo_capacity, consumables, url, created, edited))

# Vehicles Table
starwars_db_cursor.execute("SELECT id, name, model, vehicle_class, manufacturer, length, cost_in_credits, crew, passengers, max_atmosphering_speed, cargo_capacity, consumables, url, created, edited FROM vehicles")
vehicles = starwars_db_cursor.fetchall()
for vehicle in vehicles:
    id, name, model, vehicle_class, manufacturer, length, cost_in_credits, crew, passengers, max_atmosphering_speed, cargo_capacity, consumables, url, created, edited = vehicle
    insert_query = """
        INSERT INTO vehicles (id, name, model, vehicle_class, manufacturer, length, cost_in_credits, crew, passengers, max_atmosphering_speed, cargo_capacity, consumables, url, created, edited)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    normalized_db_cursor.execute(insert_query, (id, name, model, vehicle_class, manufacturer, length, cost_in_credits, crew, passengers, max_atmosphering_speed, cargo_capacity, consumables, url, created, edited))

# Species Table
starwars_db_cursor.execute("SELECT id, name, classification, designation, average_height, average_lifespan, eye_colors, hair_colors, skin_colors, language, homeworld, url, created, edited FROM species")
species = starwars_db_cursor.fetchall()
for spec in species:
    id, name, classification, designation, average_height, average_lifespan, eye_colors, hair_colors, skin_colors, language, homeworld, url, created, edited = spec
    insert_query = """
        INSERT INTO species (id, name, classification, designation, average_height, average_lifespan, eye_colors, hair_colors, skin_colors, language, homeworld, url, created, edited)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    normalized_db_cursor.execute(insert_query, (id, name, classification, designation, average_height, average_lifespan, eye_colors, hair_colors, skin_colors, language, homeworld, url, created, edited))

# Planets Table
starwars_db_cursor.execute("SELECT id, name, diameter, rotation_period, orbital_period, gravity, population, climate, terrain, surface_water, url, created, edited FROM planets")
planets = starwars_db_cursor.fetchall()
for planet in planets:
    id, name, diameter, rotation_period, orbital_period, gravity, population, climate, terrain, surface_water, url, created, edited = planet
    insert_query = """
        INSERT INTO planets (id, name, diameter, rotation_period, orbital_period, gravity, population, climate, terrain, surface_water, url, created, edited)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    normalized_db_cursor.execute(insert_query, (id, name, diameter, rotation_period, orbital_period, gravity, population, climate, terrain, surface_water, url, created, edited))

# Commit the transaction
normalized_db_conn.commit()

# ============================================================
# End of PHASE 2
# ============================================================

# ============================================================
# PHASE 3: Migrating Relationships Data and Extracting IDs from URLs for Relationship Tables
# ============================================================

# People_Films Relationship
starwars_db_cursor.execute("SELECT person_url, film_url FROM people_films")
people_films = starwars_db_cursor.fetchall()
for person_url, film_url in people_films:
    
    insert_query = """
        INSERT INTO people_films (person_url, film_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (person_url, film_url))

# People_Species Relationship
starwars_db_cursor.execute("SELECT person_url, species_url FROM people_species")
people_species = starwars_db_cursor.fetchall()
for person_url, species_url in people_species:
    
    insert_query = """
        INSERT INTO people_species (person_url, species_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (person_url, species_url))

# People_Starships Relationship
starwars_db_cursor.execute("SELECT person_url, starship_url FROM people_starships")
people_starships = starwars_db_cursor.fetchall()
for person_url, starship_url in people_starships:
    
    insert_query = """
        INSERT INTO people_starships (person_url, starship_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (person_url, starship_url))

# People_Vehicles Relationship
starwars_db_cursor.execute("SELECT person_url, vehicle_url FROM people_vehicles")
people_vehicles = starwars_db_cursor.fetchall()
for person_url, vehicle_url in people_vehicles:
    
    insert_query = """
        INSERT INTO people_vehicles (person_url, vehicle_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (person_url, vehicle_url))

# Films_Species Relationship
starwars_db_cursor.execute("SELECT film_url, species_url FROM films_species")
films_species = starwars_db_cursor.fetchall()
for film_url, species_url in films_species:
    
    insert_query = """
        INSERT INTO films_species (film_url, species_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (film_url, species_url))

# Films_Starships Relationship
starwars_db_cursor.execute("SELECT film_url, starship_url FROM films_starships")
films_starships = starwars_db_cursor.fetchall()
for film_url, starship_url in films_starships:
    
    insert_query = """
        INSERT INTO films_starships (film_url, starship_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (film_url, starship_url))

# Films_Vehicles Relationship
starwars_db_cursor.execute("SELECT film_url, vehicle_url FROM films_vehicles")
films_vehicles = starwars_db_cursor.fetchall()
for film_url, vehicle_url in films_vehicles:
    
    insert_query = """
        INSERT INTO films_vehicles (film_url, vehicle_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (film_url, vehicle_url))

# Films_Planets Relationship
starwars_db_cursor.execute("SELECT film_url, planet_url FROM films_planets")
films_planets = starwars_db_cursor.fetchall()
for film_url, planet_url in films_planets:
    
    insert_query = """
        INSERT INTO films_planets (film_url, planet_url)
        VALUES (%s, %s)
    """
    normalized_db_cursor.execute(insert_query, (film_url, planet_url))

# Commit the transaction
normalized_db_conn.commit()

# ============================================================
# End of PHASE 3
# ============================================================

# ============================================================
# PHASE 4: Migrating Ratings and Rating Providers Data from starwars_db to normalized_starwars_db
# ============================================================

# Step 1: Fetch distinct rating provider names
starwars_db_cursor.execute("SELECT DISTINCT name FROM rating_providers WHERE name IS NOT NULL")
rating_providers = starwars_db_cursor.fetchall()

# Function to convert provider name to a valid column name
def sanitize_column_name(name):
    return name.strip().lower().replace(' ', '_').replace('-', '_') + '_rating'

# Step 2: Add rating columns to the `films` table
for provider in rating_providers:
    column_name = sanitize_column_name(provider[0])
    alter_query = f"""
        ALTER TABLE films
        ADD COLUMN IF NOT EXISTS {column_name} NUMERIC
    """
    normalized_db_cursor.execute(alter_query)

# Commit to save the schema changes
normalized_db_conn.commit()

# Step 3: Fetch rating data from source
starwars_db_cursor.execute("""
    SELECT r.film_id, rp.name, r.rating_value
    FROM ratings r
    JOIN rating_providers rp ON r.rating_provider_id = rp.id
""")

ratings = starwars_db_cursor.fetchall()

# Step 4: Normalize and insert ratings into `films` table
for film_id, provider_name, rating_value in ratings:
    column_name = sanitize_column_name(provider_name)

    # Normalize the rating value
    if isinstance(rating_value, str):
        rating_value = rating_value.strip()
        if '%' in rating_value:
            rating_value = float(rating_value.strip('%'))
        elif '/' in rating_value:
            numerator, denominator = rating_value.split('/')
            rating_value = float(numerator)
            denominator = float(denominator)
            if denominator == 10:
                rating_value *= 10
        elif '.' in rating_value:
            rating_value = float(rating_value)
            if rating_value < 1:
                rating_value *= 100
            else:
                rating_value *= 10
        else:
            rating_value = float(rating_value)

    # Clamp to range 0–100
    rating_value = min(max(rating_value, 0), 100)

    # Update the rating value in the films table
    update_query = f"""
        UPDATE films
        SET {column_name} = %s
        WHERE id = %s
    """
    normalized_db_cursor.execute(update_query, (rating_value, film_id))

# Final commit
normalized_db_conn.commit()

# ============================================================
# End of PHASE 4
# ============================================================

# ============================================================
# PHASE 5: Migrating Movie Metadata to Normalized Structure
# ============================================================

# Fetch all movie metadata from the old movie_metadata table
starwars_db_cursor.execute("""
    SELECT id, popularity, keywords, overview, runtime
    FROM movie_metadata
""")

movie_metadata_rows = starwars_db_cursor.fetchall()

# Insert keywords into the normalized `keywords` table and film details into the `films` table
for metadata in movie_metadata_rows:
    film_id, popularity, keywords, overview, runtime = metadata

    # Insert `overview` and `runtime` into the `films` table
    normalized_db_cursor.execute("""
        UPDATE films 
        SET overview = %s, runtime = %s
        WHERE id = %s
    """, (overview, runtime, film_id))

    # Split the keywords into a list (assuming they are comma-separated)
    keyword_list = keywords.split(',') if keywords else []
    
    for keyword in keyword_list:
        # Strip whitespace and handle duplicates
        keyword = keyword.strip()
        
        if keyword:
            # Check if the keyword already exists in the `keywords` table
            normalized_db_cursor.execute("SELECT id FROM keywords WHERE keyword = %s", (keyword,))
            keyword_row = normalized_db_cursor.fetchone()
            
            if keyword_row:
                keyword_id = keyword_row[0]
            else:
                # Insert new keyword if it doesn't exist
                normalized_db_cursor.execute("INSERT INTO keywords (keyword) VALUES (%s) RETURNING id", (keyword,))
                keyword_id = normalized_db_cursor.fetchone()[0]

            # Insert into the new movie_metadata table
            normalized_db_cursor.execute("""
                INSERT INTO movie_metadata (film_id, keyword_id)
                VALUES (%s, %s)
            """, (film_id, keyword_id))

# Commit the changes to the normalized database
normalized_db_conn.commit()

# ============================================================
# End of PHASE 5
# ============================================================


# Close the cursors and connections
starwars_db_cursor.close()
normalized_db_cursor.close()
starwars_db_conn.close()
normalized_db_conn.close()

print("Films table populated successfully in normalized_starwars_db!")
