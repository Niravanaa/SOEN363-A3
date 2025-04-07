MATCH (f:Film)
WHERE f.release_date > "1980-01-01" 
  AND toInteger(f.internet_movie_database_rating) >= 50
RETURN f.title, f.release_date, f.internet_movie_database_rating;
