MATCH (f:Film {title: "A New Hope"})
WITH f, f.planets AS planet_urls
UNWIND planet_urls AS planet_url
MATCH (p:Planet {url: planet_url})
RETURN p.name AS planet_name;
