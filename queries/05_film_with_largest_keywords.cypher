MATCH (f:Film)
WITH f, size(f.keywords) AS num_keywords
ORDER BY num_keywords DESC
LIMIT 1
RETURN f.title, num_keywords;
