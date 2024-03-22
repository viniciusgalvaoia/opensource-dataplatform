-- internal_movies_parquet
DROP TABLE minio.landing.internal_movies_parquet;
CREATE TABLE IF NOT EXISTS minio.landing.internal_movies_parquet (
    title VARCHAR,
    duration_mins INTEGER,
    original_language VARCHAR,
    size_mb INTEGER
) WITH (
    external_location = 's3a://landing/strider/internal/movies/parquet',
    format = 'PARQUET'
);

-- internal_streams_parquet
DROP TABLE minio.landing.internal_streams_parquet;
CREATE TABLE minio.landing.internal_streams_parquet (
    movie_title VARCHAR,
    user_email VARCHAR,
    size_mb DOUBLE,
    start_at VARCHAR,
    end_at VARCHAR
) WITH (
    external_location = 's3a://landing/strider/internal/streams/parquet',
    format = 'PARQUET'
);

-- internal_users_parquet
DROP TABLE minio.landing.internal_users_parquet;
CREATE TABLE minio.landing.internal_users_parquet (
    first_name VARCHAR,
    last_name VARCHAR,
    email VARCHAR
) WITH (
    external_location = 's3a://landing/strider/internal/users/parquet',
    format = 'PARQUET'
);


-- vendor_authors_parquet
DROP TABLE minio.landing.vendor_authors_parquet;
CREATE TABLE minio.landing.vendor_authors_parquet (
    metadata ROW(
        name VARCHAR,
        birth_date VARCHAR,
        died_at VARCHAR
    ),
    nationalities ARRAY[ROW(
        id INTEGER, 
        label VARCHAR, 
        slug VARCHAR
    )]
) WITH (
    external_location = 's3a://landing/strider/vendor/authors/parquet',
    format = 'PARQUET'
);

-- vendor_books_parquet
DROP TABLE minio.landing.vendor_books_parquet;
CREATE TABLE minio.landing.vendor_books_parquet (
    name VARCHAR,
    pages INTEGER,
    author VARCHAR,
    publisher VARCHAR
) WITH (
    external_location = 's3a://landing/strider/vendor/books/parquet',
    format = 'PARQUET'
);

-- vendor_reviews_parquet
DROP TABLE minio.landing.vendor_reviews_parquet;
CREATE TABLE minio.landing.vendor_reviews_parquet (
    content ROW(text VARCHAR),
    rating ROW(rate INTEGER, label VARCHAR),
    books ARRAY(ROW(
        id INTEGER,
        metadata ROW(title VARCHAR, pages VARCHAR)
    )),
    movies ARRAY(ROW(
        id INTEGER,
        title VARCHAR
    )),
    updated VARCHAR,
    created VARCHAR
) WITH (
    external_location = 's3a://landing/strider/vendor/reviews/parquet',
    format = 'PARQUET'
);
