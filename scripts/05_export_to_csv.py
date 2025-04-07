import os
from dotenv import load_dotenv
import psycopg2
import csv

# Load environment variables from the .env file
load_dotenv()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("NORMALIZED_DB_NAME"),
    user= os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

# Create a cursor to interact with the database
cursor = conn.cursor()

# Define a function to fetch data and write it to CSV
def export_to_csv(query, filename):
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write headers based on the query column names
        writer.writerow([desc[0] for desc in cursor.description])
        writer.writerows(rows)
    print(f"Data exported to {filename}")

# ===========================
# Export Films Table
# ===========================
films_query = """
SELECT *
FROM films;
"""
export_to_csv(films_query, 'migration-csv-tables/films.csv')

# ===========================
# Export People Table
# ===========================
people_query = """
SELECT *
FROM people;
"""
export_to_csv(people_query, 'migration-csv-tables/people.csv')

# ===========================
# Export Starships Table
# ===========================
starships_query = """
SELECT *
FROM starships;
"""
export_to_csv(starships_query, 'migration-csv-tables/starships.csv')

# ===========================
# Export Vehicles Table
# ===========================
vehicles_query = """
SELECT *
FROM vehicles;
"""
export_to_csv(vehicles_query, 'migration-csv-tables/vehicles.csv')

# ===========================
# Export Species Table
# ===========================
species_query = """
SELECT *
FROM species;
"""
export_to_csv(species_query, 'migration-csv-tables/species.csv')

# ===========================
# Export Planets Table
# ===========================
planets_query = """
SELECT *
FROM planets;
"""
export_to_csv(planets_query, 'migration-csv-tables/planets.csv')

# ===========================
# Export Keywords Table
# ===========================
keywords_query = """
SELECT id, keyword FROM keywords;
"""
export_to_csv(keywords_query, 'migration-csv-tables/keywords.csv')

# ===========================
# Export Movie Metadata Table
# ===========================
movie_metadata_query = """
SELECT film_id, keyword_id FROM movie_metadata;
"""
export_to_csv(movie_metadata_query, 'migration-csv-tables/movie_metadata.csv')

# ===========================
# Export Rating Providers Table
# ===========================
rating_providers_query = """
SELECT id, name FROM rating_providers;
"""
export_to_csv(rating_providers_query, 'migration-csv-tables/rating_providers.csv')

# ===========================
# Export Ratings Table
# ===========================
ratings_query = """
SELECT film_id, rating_provider_id, rating_value FROM ratings;
"""
export_to_csv(ratings_query, 'migration-csv-tables/ratings.csv')

def export_aggregated_relation(query, filename):
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([desc[0] for desc in cursor.description])
        writer.writerows(rows)
    print(f"Exported: {filename}")

# -----------------------
# People → Films
# -----------------------
export_aggregated_relation("""
    SELECT person_url, string_agg(film_url, ',') AS films
    FROM people_films
    GROUP BY person_url
""", "migration-csv-tables/people_films_array.csv")

# -----------------------
# People → Species
# -----------------------
export_aggregated_relation("""
    SELECT person_url, string_agg(species_url, ',') AS species
    FROM people_species
    GROUP BY person_url
""", "migration-csv-tables/people_species_array.csv")

# -----------------------
# People → Starships
# -----------------------
export_aggregated_relation("""
    SELECT person_url, string_agg(starship_url, ',') AS starships
    FROM people_starships
    GROUP BY person_url
""", "migration-csv-tables/people_starships_array.csv")

# -----------------------
# People → Vehicles
# -----------------------
export_aggregated_relation("""
    SELECT person_url, string_agg(vehicle_url, ',') AS vehicles
    FROM people_vehicles
    GROUP BY person_url
""", "migration-csv-tables/people_vehicles_array.csv")

# -----------------------
# Films → Species
# -----------------------
export_aggregated_relation("""
    SELECT film_url, string_agg(species_url, ',') AS species
    FROM films_species
    GROUP BY film_url
""", "migration-csv-tables/films_species_array.csv")

# -----------------------
# Films → Starships
# -----------------------
export_aggregated_relation("""
    SELECT film_url, string_agg(starship_url, ',') AS starships
    FROM films_starships
    GROUP BY film_url
""", "migration-csv-tables/films_starships_array.csv")

# -----------------------
# Films → Vehicles
# -----------------------
export_aggregated_relation("""
    SELECT film_url, string_agg(vehicle_url, ',') AS vehicles
    FROM films_vehicles
    GROUP BY film_url
""", "migration-csv-tables/films_vehicles_array.csv")

# -----------------------
# Films → Planets
# -----------------------
export_aggregated_relation("""
    SELECT film_url, string_agg(planet_url, ',') AS planets
    FROM films_planets
    GROUP BY film_url
""", "migration-csv-tables/films_planets_array.csv")

# -----------------------
# Films → Keywords
# -----------------------
export_aggregated_relation("""
    SELECT f.url AS film_url, string_agg(k.keyword, ',') AS keywords
    FROM movie_metadata mm
    JOIN films f ON mm.film_id = f.id
    JOIN keywords k ON mm.keyword_id = k.id
    GROUP BY f.url
""", "migration-csv-tables/films_keywords_array.csv")

# Close the cursor and connection
cursor.close()
conn.close()

print("All data exported successfully!")
