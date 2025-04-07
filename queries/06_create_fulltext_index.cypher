DROP INDEX filmOverviewIndex IF EXISTS;
CREATE FULLTEXT INDEX filmOverviewIndex FOR (f:Film) ON EACH [f.overview];
