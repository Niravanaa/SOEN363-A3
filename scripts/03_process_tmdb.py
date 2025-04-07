import csv
import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

conn = psycopg2.connect(
    dbname=os.getenv("UNNORMALIZED_DB_NAME"),
    user= os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()

SCHEMA_FILE = "ddl/03_tmdb_tables.sql"

try:
    cursor = conn.cursor()

    # Read the SQL schema file
    with open(SCHEMA_FILE, "r", encoding="utf-8") as file:
        sql_script = file.read()

    # Execute the SQL script
    cursor.execute(sql_script)
    conn.commit()

    print("TMDB Schema executed successfully.")

except Exception as e:
    print(f"Error executing schema: {e}")

# Path to the CSV file
current_directory = os.getcwd()
csv_file_path = os.path.join(current_directory, "data", "tmdb_filtered_dataset.csv")

# Read IMDb IDs from ratings table and map them to film ids
print("Fetching IMDb IDs and corresponding film ids from ratings table...")
cur.execute("""
    SELECT r.imdb_id, f.id 
    FROM ratings r
    JOIN films f ON r.film_id = f.id;
""")
imdb_to_film_id = {row[0]: row[1] for row in cur.fetchall()}
print(f"Retrieved {len(imdb_to_film_id)} IMDb IDs and film IDs.")

# Read CSV and insert relevant data
with open(csv_file_path, mode="r", encoding="utf-8") as csv_file:
    reader = csv.DictReader(csv_file)

    for row in reader:
        imdb_id = row.get("imdb_id")
        popularity = row.get("popularity")
        keywords = row.get("keywords")
        overview = row.get("overview")
        runtime = row.get("runtime")

        # Skip rows with missing data or if imdb_id is not found in ratings table
        if not imdb_id or imdb_id not in imdb_to_film_id or not popularity or not keywords:
            continue

        film_id = imdb_to_film_id[imdb_id]

        print(f"Inserting film_id {film_id} (IMDb ID: {imdb_id}) with popularity {popularity} and keywords {keywords}")

        # Insert into the movie_metadata table, ensuring the film_id is used as a reference
        cur.execute(
            """
            INSERT INTO movie_metadata (id, imdb_id, popularity, keywords, overview, runtime)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
            """,
            (film_id, imdb_id, popularity, keywords, overview, runtime)
        )
        conn.commit()

print("Closing database connection...")
cur.close()
conn.close()
print("Movie metadata populated successfully.")
