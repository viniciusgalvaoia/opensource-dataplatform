SHOW CATALOGS;

CREATE SCHEMA minio.landing
WITH (location = 's3://landing/')

SHOW SCHEMAS FROM minio;
SHOW TABLES FROM minio.landing;