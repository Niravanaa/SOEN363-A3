import requests
import psycopg2
from dotenv import load_dotenv
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Database connection
conn = psycopg2.connect(
    dbname=os.getenv("UNNORMALIZED_DB_NAME"),
    user= os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()

# OMDB API Key
OMDB_BASE_URL = "http://www.omdbapi.com/"
SCHEMA_FILE = "ddl/02_omdb_tables.sql"

try:
    cursor = conn.cursor()

    # Read the SQL schema file
    with open(SCHEMA_FILE, "r", encoding="utf-8") as file:
        sql_script = file.read()

    # Execute the SQL script
    cursor.execute(sql_script)
    conn.commit()

    print("OMDB Schema executed successfully.")

except Exception as e:
    print(f"Error executing schema: {e}")

# Fetch all films (title and release year)
print("Fetching films from database...")
cur.execute("SELECT id, title, release_date FROM films;")
films = cur.fetchall()
print(f"Retrieved {len(films)} films.")

# Function to get movie ratings from OMDB API
def fetch_movie_ratings(title, year):
    params = {
        "t": title,
        "y": year,
        "apikey": os.getenv("OMDB_API_KEY")
    }
    print(f"Requesting OMDB API for {title} ({year})...")
    response = requests.get(OMDB_BASE_URL, params=params)
    if response.status_code == 200:
        print(f"Response received for {title} ({year})")
        return response.json()
    else:
        print(f"Failed to fetch data for {title} ({year}) - Status Code: {response.status_code}")
        return None

# Insert rating providers
print("Fetching existing rating providers...")
cur.execute("SELECT id, name FROM rating_providers;")
existing_providers = {name: id for id, name in cur.fetchall()}
print(f"Retrieved {len(existing_providers)} rating providers.")

# Process each film
for film_id, title, release_date in films:
    year = release_date.year if release_date else None
    if not year:
        print(f"Skipping {title} due to missing release year.")
        continue

    print(f"Fetching ratings for {title} ({year})")
    movie_data = fetch_movie_ratings(title, year)
    
    if not movie_data or movie_data.get("Response") == "False":
        print(f"No data found for {title} ({year}).")
        continue
    
    imdb_id = movie_data.get("imdbID", "N/A")
    print(f"IMDb ID for {title} ({year}): {imdb_id}")
    
    print(f"Processing ratings for {title} ({year})")
    for rating in movie_data.get("Ratings", []):
        provider_name = rating["Source"]
        rating_value = rating["Value"]
        print(f"Found rating: {provider_name} - {rating_value}")
        
        # Ensure provider exists
        if provider_name not in existing_providers:
            print(f"Inserting new rating provider: {provider_name}")
            cur.execute("INSERT INTO rating_providers (name) VALUES (%s) RETURNING id;", (provider_name,))
            provider_id = cur.fetchone()[0]
            existing_providers[provider_name] = provider_id
            conn.commit()
        else:
            provider_id = existing_providers[provider_name]
        
        # Insert rating with imdb_id
        print(f"Inserting rating for {title}: IMDb ID {imdb_id}, Provider ID {provider_id}, Value {rating_value}")
        cur.execute(
            """
            INSERT INTO ratings (film_id, imdb_id, rating_provider_id, rating_value)
            VALUES (%s, %s, %s, %s);
            """,
            (str(film_id), str(imdb_id), provider_id, rating_value)  # Ensuring correct types
        )
        conn.commit()

print("Closing database connection...")
cur.close() 
conn.close()
print("Ratings data populated successfully.")
