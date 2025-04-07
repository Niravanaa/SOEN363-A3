CALL db.index.fulltext.queryNodes("filmOverviewIndex", "galaxy")
YIELD node, score
RETURN node.title AS title, node.overview AS overview, score
ORDER BY score DESC
LIMIT 10;
