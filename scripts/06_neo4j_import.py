from neo4j import GraphDatabase
import os
import csv
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Path to CSVs
CSV_DIR = "migration-csv-tables"

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

# Phase 1: Create nodes from CSVs with dynamic fields
def create_nodes(tx, label, csv_file):
    with open(os.path.join(CSV_DIR, csv_file), newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        for row in reader:
            props = {k: row[k] for k in headers}
            prop_str = ", ".join(f"{k}: ${k}" for k in headers)
            query = f"MERGE (n:{label} {{ {prop_str} }})"
            tx.run(query, **props)

# Phase 2: Add arrays as node attributes using dynamic detection
def add_array_property(tx, label, csv_file):
    with open(os.path.join(CSV_DIR, csv_file), newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        if len(headers) != 2:
            raise ValueError(f"Expected exactly 2 columns in {csv_file}, got {headers}")
        id_field, array_field = headers

        for row in reader:
            identifier = row[id_field]
            array_data = row[array_field].split(",") if row[array_field] else []
            query = f"""
                MATCH (n:{label} {{url: $id}})
                SET n.{array_field} = $items
            """
            tx.run(query, id=identifier, items=array_data)

with driver.session() as session:

    # ========= Phase 1: Import Nodes =========
    print("Importing node data...")

    session.execute_write(create_nodes, "Film", "films.csv")
    session.execute_write(create_nodes, "Person", "people.csv")
    session.execute_write(create_nodes, "Starship", "starships.csv")
    session.execute_write(create_nodes, "Vehicle", "vehicles.csv")
    session.execute_write(create_nodes, "Species", "species.csv")
    session.execute_write(create_nodes, "Planet", "planets.csv")

    print("Nodes imported!")

    # ========= Phase 2: Add arrays =========
    print("Adding array attributes...")

    session.execute_write(add_array_property, "Person", "people_films_array.csv")
    session.execute_write(add_array_property, "Person", "people_species_array.csv")
    session.execute_write(add_array_property, "Person", "people_starships_array.csv")
    session.execute_write(add_array_property, "Person", "people_vehicles_array.csv")

    session.execute_write(add_array_property, "Film", "films_species_array.csv")
    session.execute_write(add_array_property, "Film", "films_starships_array.csv")
    session.execute_write(add_array_property, "Film", "films_vehicles_array.csv")
    session.execute_write(add_array_property, "Film", "films_planets_array.csv")
    session.execute_write(add_array_property, "Film", "films_keywords_array.csv")

    print("Array attributes added!")

driver.close()
print("Neo4j import complete.")
