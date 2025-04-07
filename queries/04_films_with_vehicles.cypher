MATCH (f:Film)
MATCH (v:Vehicle)
WHERE v.name IN ['Sith speeder', 'Koro-2 Exodrive airspeeder'] 
AND v.url IN f.vehicles
RETURN f.title, f.release_date, COLLECT(v.name) AS vehicles;
