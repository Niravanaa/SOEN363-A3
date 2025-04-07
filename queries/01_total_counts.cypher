// Total number of films
MATCH (f:Film)
RETURN count(f) AS total_films;

// Total number of planets
MATCH (p:Planet)
RETURN count(p) AS total_planets;

// Total number of species
MATCH (s:Species)
RETURN count(s) AS total_species;
