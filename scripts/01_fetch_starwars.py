import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Access the environment variables
DB_PARAMS = {
    "dbname": os.getenv("UNNORMALIZED_DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

BASE_URL = "https://swapi.dev/api/"
SCHEMA_FILE = "ddl/01_starwars_api_tables.sql"

try:
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Read the SQL schema file
    with open(SCHEMA_FILE, "r", encoding="utf-8") as file:
        sql_script = file.read()

    # Execute the SQL script
    cursor.execute(sql_script)
    conn.commit()

    print("Star Wars Schema executed successfully.")

except Exception as e:
    print(f"Error executing schema: {e}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if conn:
        conn.close()

# Connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

# Fetch data from SWAPI with pagination
def fetch_data(endpoint):
    url = BASE_URL + endpoint
    data = []
    while url:
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            data.extend(json_data["results"])
            url = json_data["next"]  # Pagination
        else:
            print(f"Failed to fetch {endpoint}: {response.status_code}")
            break
    return data

# Insert data into database
def insert_data(table, data, columns):
    conn = get_db_connection()
    cur = conn.cursor()

    # Exclude relationship columns from the insert data
    non_relationship_columns = [col for col in columns if col not in ["films", "species", "starships", "vehicles", "planets"]]

    query = f"""
        INSERT INTO {table} ({', '.join(non_relationship_columns)})
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    values = []
    for record in data:
        row = []
        for col in non_relationship_columns:
            if isinstance(record.get(col), list):
                row.append("{" + ",".join(record.get(col, [])) + "}")  # Convert list to PostgreSQL array format
            else:
                row.append(record.get(col, None))
        values.append(row)

    try:
        print("Inserting non-relationship data into table:", table)
        execute_values(cur, query, values)
        conn.commit()

    except Exception as e:
        print(f"Error inserting non-relationship data into {table}: {e}")
        conn.rollback()  # Rollback on error
    finally:
        cur.close()

    return conn, table, data, columns

def insert_relationship_data(conn, table, data, columns):
    cur = conn.cursor()

    # Phase 2: Insert relationship data for all tables
    if "films" in columns:
        for record in data:
            for film_url in record.get("films", []):
                print(f"Inserting into people_films: person_url={record['url']} film_url={film_url}")
                execute_values(cur, f"""
                    INSERT INTO people_films (person_url, film_url)
                    VALUES %s
                """, [(record["url"], film_url)])

    # Insert relationships for "species"
    if "species" in columns:
        for record in data:
            for species_url in record.get("species", []):
                if table == "people":  # Inserting into people_starships
                    print(f"Inserting into people_species: person_url={record['url']} species_url={species_url}")
                    execute_values(cur, f"""
                        INSERT INTO people_species (person_url, species_url)
                        VALUES %s
                    """, [(record["url"], species_url)])
                elif table == "films":  # Inserting into films_starships
                    print(f"Inserting into films_species: film_url={record['url']} species_url={species_url}")
                    execute_values(cur, f"""
                        INSERT INTO films_species (film_url, species_url)
                        VALUES %s
                    """, [(record["url"], species_url)])

    # Insert relationships for "starships"
    if "starships" in columns:
        for record in data:
            for starship_url in record.get("starships", []):
                if table == "people":  # Inserting into people_starships
                    print(f"Inserting into people_starships: person_url={record['url']} starship_url={starship_url}")
                    execute_values(cur, f"""
                        INSERT INTO people_starships (person_url, starship_url)
                        VALUES %s
                    """, [(record["url"], starship_url)])
                elif table == "films":  # Inserting into films_starships
                    print(f"Inserting into films_starships: film_url={record['url']} starship_url={starship_url}")
                    execute_values(cur, f"""
                        INSERT INTO films_starships (film_url, starship_url)
                        VALUES %s
                    """, [(record["url"], starship_url)])

    # Insert relationships for "vehicles"
    if "vehicles" in columns:
        for record in data:
            for vehicle_url in record.get("vehicles", []):
                if table == "people":  # Inserting into people_vehicles
                    print(f"Inserting into people_vehicles: person_url={record['url']} vehicle_url={vehicle_url}")
                    execute_values(cur, f"""
                        INSERT INTO people_vehicles (person_url, vehicle_url)
                        VALUES %s
                    """, [(record["url"], vehicle_url)])
                elif table == "films":  # Inserting into films_vehicles
                    print(f"Inserting into films_vehicles: film_url={record['url']} vehicle_url={vehicle_url}")
                    execute_values(cur, f"""
                        INSERT INTO films_vehicles (film_url, vehicle_url)
                        VALUES %s
                    """, [(record["url"], vehicle_url)])

    # Insert relationships for "planets"
    if "planets" in columns:
        for record in data:
            for planet_url in record.get("planets", []):
                print(f"Inserting into films_planets: film_url={record['url']} planet_url={planet_url}")
                execute_values(cur, f"""
                    INSERT INTO films_planets (film_url, planet_url)
                    VALUES %s
                """, [(record["url"], planet_url)])

    # Commit the relationship data after all inserts
    conn.commit()

def fetch_data_by_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch {url}: {response.status_code}")
        return []

# Fetch and store Star Wars data
def main():
    tables = {
        "people": ["name", "birth_year", "eye_color", "gender", "hair_color", "height", "mass", "skin_color", "homeworld", "url", "created", "edited", "films", "species", "starships", "vehicles"],
        "films": ["title", "episode_id", "opening_crawl", "director", "producer", "release_date", "url", "created", "edited", "species", "starships", "vehicles", "planets"],
        "planets": ["name", "diameter", "rotation_period", "orbital_period", "gravity", "population", "climate", "terrain", "surface_water", "url", "created", "edited"],
        "species": ["name", "classification", "designation", "average_height", "average_lifespan", "eye_colors", "hair_colors", "skin_colors", "language", "homeworld", "url", "created", "edited"],
        "vehicles": ["name", "model", "vehicle_class", "manufacturer", "length", "cost_in_credits", "crew", "passengers", "max_atmosphering_speed", "cargo_capacity", "consumables", "url", "created", "edited"],
        "starships": ["name", "model", "starship_class", "manufacturer", "cost_in_credits", "length", "crew", "passengers", "max_atmosphering_speed", "hyperdrive_rating", "MGLT", "cargo_capacity", "consumables", "url", "created", "edited"]
    }
    
    # Phase 1: Insert non-relationship data for all tables
    all_connections = []
    for table, columns in tables.items():
        print(f"Fetching {table}...")
        data = fetch_data(table)
        if data:
            print(f"Inserting {len(data)} records into {table}...")
            conn, table, data, columns = insert_data(table, data, columns)
            all_connections.append((conn, table, data, columns))
        else:
            print(f"No data fetched for {table}.")

    # Phase 2: Insert relationship data for all tables
    for conn, table, data, columns in all_connections:
        print(f"Inserting relationship data for {table}...")
        insert_relationship_data(conn, table, data, columns)
        conn.close()  # Close each connection after handling it

if __name__ == "__main__":
    main()
