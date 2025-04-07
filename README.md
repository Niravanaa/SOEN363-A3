![ReadmeBanner](https://i.imgur.com/K8GVco9.png)

# SOEN 363: Data Systems for Software Engineers - Assignment 3

## Overview

In this assignment, I created a NoSQL database using Neo4j to store and manage movie-related data, including films, people, planets, species, vehicles, and starships. I first extracted the necessary data from a relational database (from a previous assignment) into CSV format and then imported it into Neo4j. I wrote and executed Cypher queries to perform various tasks, such as counting the total number of films and planets, retrieving films based on specific conditions, identifying the film with the most keywords, and building a full-text search index to query movie overviews.

## Getting Started

Follow these steps to get your development environment up and running for this assignment.

### Prerequisites

Make sure you have the following installed and set up:

- PostgreSQL - For implementing and managing the database.
- Python 3.x - If you're using Python for API interactions.
- Python Libraries:
  - neo4j - Python driver for interacting with Neo4j.
  - psycopg2 - PostgreSQL adapter for Pytho
- Neo4j - For running queries in the unstructured graph database. Install Neo4j Desktop or Neo4j Aura (cloud version) for local or cloud-based graph database solutions.

### Installing

1. Install Python Dependencies: If you're using Python for API interaction, install the required libraries:

```bash
pip install psycopg2 neo4j requests
```

2. Set Up PostgreSQL: Install and set up PostgreSQL on your local machine if it's not installed already. Use pgAdmin or the PostgreSQL CLI to manage your database.

3. Install and Set Up Neo4j:

   - Download and install Neo4j Desktop from Neo4j’s website.
   - Alternatively, use Neo4j Aura for a cloud-based solution.
   - After installation, create a new Neo4j project and start a Neo4j database instance.

4. Set Up the Database:
   Create a PostgreSQL database for this project.
   Use DDL scripts (located in the `ddl` folder) to set up the initial tables based on the Star Wars API, OMDB API, and TMDB datasets.

5. Setting up the environment variables: Your `.env` file should look like the following:

```plaintext
UNNORMALIZED_DB_NAME=
NORMALIZED_DB_NAME=
DB_USER=
DB_PASSWORD=
DB_HOST=
DB_PORT=
OMDB_API_KEY=
NEO4J_URI=
NEO4J_USER=
NEO4J_PASSWORD=
```

6. Run the PostgreSQL Data Import:

   - Extract and convert the relational data from PostgreSQL into CSV files using the provided export_to_csv.py script.
   - Make sure that the migration-csv-tables folder contains all necessary CSVs with your data.

7. Import Data into Neo4j:

   - Use the Cypher queries located in the queries folder to import the data into Neo4j.
   - The queries include creating nodes and relationships for each movie-related entity (e.g., films, people, planets, species, etc.).

8. Running Queries:

   - The provided Python script run_queries.py can execute queries from the queries folder, such as calculating the total number of films, finding films by specific criteria, and creating indexes.
   - Make sure the Neo4j instance is running before executing the queries.

9. Full-Text Search in Neo4j:
   - One of the required tasks in the assignment involves setting up a full-text index on the movie overview using Cypher. This allows you to run efficient text-based searches across large movie datasets.

> [!NOTE]
> Alternatively, the [Jupyter Notebook](A3_NiravPatel_40248940.ipynb) file to run the code and view the query results without having to run it locally.

### Additional Requirements

In regards to the ERD diagrams, the `erd` folder contains both the unnormalized and normalized ERD diagrams.

In regards to the DML scripts, the `dml` folder contains the DML scripts that can be used to populate both unnormalized and normalized databases.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Thanks to the Star Wars API for providing the movie data.
- Thanks to OMDB and Kaggle for their respective APIs and datasets.
