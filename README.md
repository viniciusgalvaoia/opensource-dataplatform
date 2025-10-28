# Open Source Data Platform

## **Overview**

In order to answer some important questions about the product of small startup streaming app in the month of December 2021, a data platform was created with a focus on processing large volumes of data while being able to control and predict costs and easy to install on any of the cloud providers (AWS, GCP and Azure, for example).

## Data Product
In order to have a data product that is agnostic to any cloud provider, the following tools were chosen, all running on Kubernetes:

![ETL Pipeline](https://raw.githubusercontent.com/viniciusgalvaoia/opensource-dataplatform/main/imgs/dataplatform.PNG)

- **ArgoCD**

A declarative, GitOps continuous delivery tool for Kubernetes, which we'll provision and manage all data applications through it.

![ArgoCd](https://raw.githubusercontent.com/viniciusgalvaoia/opensource-dataplatform/main/imgs/argocd.PNG)

- **MinIO**

 Offers cost-effective, scalable, high-performance, and secure object storage, compatible with S3, with easy deployment and strong community support.

 ![MinIO](https://raw.githubusercontent.com/viniciusgalvaoia/opensource-dataplatform/main/imgs/minio.PNG)

- **Trino**

With Trino, we have a fast query performance, scalability, compatibility with various data sources, support for complex queries, and a vibrant open-source community. 

 ![Trino](https://raw.githubusercontent.com/viniciusgalvaoia/opensource-dataplatform/main/imgs/trino.PNG)

 - **Airflow** + **dbt**

 ![Airflow](https://raw.githubusercontent.com/viniciusgalvaoia/opensource-dataplatform/main/imgs/airflow.PNG)

 Using Airflow and dbt, enables version control and reproducibility of data transformations and orchestrations. 


 ## How to use


This project can be divided into 3 parts:
- infra
- apps
- data


### **Infra**
All the resources needed to create a Kubernetes cluster as well as the necessary components for the data environment using GitOps.

To deploy the environment, follow the steps:

#### Kubernetes
azure

```
az login --use-device-code
az account set --subscription "<your-subscription>"
```

provision

```
terraform --version
terraform init
terraform plan
terraform apply
az aks get-credentials --resource-group rg-orion --name orion-development --overwrite-existing
```

#### GitOps
build
```
terraform init
terraform apply
```
configure (manual)
```
kubectl patch svc argocd-server -n gitops -p '{"spec": {"type": "LoadBalancer"}}'
kubectl get svc argocd-server -n gitops
kubectl -n gitops get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d; echo

argocd login "lb" --username admin --password "pwd" --insecure
argocd cluster add "name"

kubectl apply -f git-repo-con.yaml -n gitops
```

#### Deployments

minio [deepstorage]
```
kubectl apply -f app-manifests/deepstorage/minio-operator.yaml
kubectl apply -f app-manifests/deepstorage/minio-tenant.yaml
```

hive metastore [metastore]
```sh
kubectl apply -f app-manifests/metastore/hive-metastore.yaml
```

trino [warehouse]
```sh
kubectl apply -f app-manifests/warehouse/trino.yaml
```

airflow [orchestrator]
```sh
kubectl create secret generic airflow-fernet-key --from-literal=fernet-key='<your-key>=' --namespace orchestrator
kubectl apply -f git-credentials-secret.yaml --namespace orchestrator
kubectl apply -f app-manifests/orchestrator/airflow.yaml
```

### Apps
Development of an application that reads files from vendors and internals to place in the landing zone folder of a data lake, in this case using MinIO (s3).

To run the application:
```sh
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python cli.py all parquet
```

### Data
Creating a data pipeline using Apache Airflow, Trino & dbt-Core to create a data environment complete end to end.

To build the data environment, follow the steps:

- Trino
```sql
CREATE SCHEMA minio.landing
WITH (location = 's3://landing/')

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
    nationalities array(ROW(
        id INTEGER, 
        label VARCHAR, 
        slug VARCHAR
    ))
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
```


- dbt_Core

Six dbt models were created on stage layer using merge as incremental strategy:
1. internal_movies.sql
2. internal_streams.sql
3. internal_users.sql
4. vendors_authors.sql
5. vendors_books.sql
6. vendors_reviews.sql

#### Analytical queries on trusted layer

#### 1. What percentage of the streamed movies are based on books?
```sql
{{ config(
    materialized = 'table',
    database = 'iceberg',
	schema='trusted'
) }}

WITH movies_based_on_books AS (
	SELECT DISTINCT
		LOWER(movie_title) as movie_title
    FROM
		{{ ref('vendor_reviews') }}
),
streamed_movies AS (
	SELECT DISTINCT
		LOWER(movie_title) as movie_title
    FROM
		{{ ref('internal_streams') }}
)
SELECT
	COUNT(*) total_streamed_movies,
    COUNT(movies_based_on_books.movie_title) total_streamed_movies_based_on_books,
    (CAST(COUNT(movies_based_on_books.movie_title) AS DOUBLE) / COUNT(*))  percentage_of_streamed_movies_based_on_books
FROM
	streamed_movies
LEFT JOIN
	movies_based_on_books
		ON streamed_movies.movie_title = movies_based_on_books.movie_title
```
| result |
| :--- |
| 0.934 |


#### 2. During Christmas morning (7 am and 12 noon on December 25), a partial system outage was caused by a corrupted file. Knowing the file was part of the movie "Unforgiven" thus could affect any in-progress streaming session of that movie, how many users were potentially affected?
```sql
{{ config(
    materialized = 'table',
    database = 'iceberg',
	schema='trusted'
) }}

SELECT
	count(DISTINCT user_email) total_users_affected
FROM
	{{ ref('internal_streams') }}
WHERE
	movie_title = 'Unforgiven'
	AND (
		cast(concat(substring(start_at, 1, 10),
	' ',
	substring(start_at, 12, 8)) AS TIMESTAMP) BETWEEN TIMESTAMP '2021-12-25 07:00:00' and TIMESTAMP '2021-12-25 12:00:00'
		or cast(concat(substring(end_at, 1, 10),
		' ',
		substring(end_at, 12, 8)) AS TIMESTAMP) BETWEEN TIMESTAMP '2021-12-25 07:00:00' and TIMESTAMP '2021-12-25 12:00:00'
    )

```
| result |
| :--- |
|    4   |


#### 3. How many movies based on books written by Singaporeans authors were streamed that month?
```sql
{{ config(
    materialized = 'table',
    database = 'iceberg',
	schema='trusted'
) }}

WITH movies_based_on_books AS (
	SELECT DISTINCT
		LOWER(movie_title) as movie_title,
        LOWER(book_title) as book_title
    FROM
		{{ ref('vendor_reviews') }}
),
streamed_movies AS (
	SELECT DISTINCT
		LOWER(movie_title) as movie_title
    FROM
		{{ ref('internal_streams') }}
)
SELECT
	COUNT(1) movies_based_on_books_written_by_singaporeans
FROM
	streamed_movies	
INNER JOIN
	movies_based_on_books
		ON streamed_movies.movie_title = movies_based_on_books.movie_title
INNER JOIN
	{{ ref('vendor_books') }}
		ON movies_based_on_books.book_title = LOWER(vendor_books.name)
INNER JOIN
	{{ ref('vendor_authors') }}
		ON LOWER(vendor_books.author) = LOWER(vendor_authors.name)
        AND vendor_authors.nationalitiy_slug = 'singaporeans'
```
| result |
| :--- |
| 3 |


#### 4. What's the average streaming duration?
```sql
{{ config(
    materialized = 'table',
    database = 'iceberg',
    schema='trusted'
) }}

SELECT 
    AVG(CAST(date_diff('hour', cast(concat(substring(start_at, 1, 10),' ', substring(start_at, 12, 8)) AS TIMESTAMP), 
    cast(concat(substring(end_at, 1, 10),' ', substring(end_at, 12, 8)) AS TIMESTAMP)) AS double)) AS average_streaming_duration
FROM 
	{{ ref('internal_streams') }}
```
| result |
| :--- |
| 11538 |


#### 5. What's the median streaming size in gigabytes?
```sql
{{ config(
    materialized = 'table',
    database = 'iceberg',
    schema='trusted'
) }}
SELECT 
	ROUND((approx_percentile(size_mb, 0.5) / 1000), 2) AS median_size_in_gb
FROM 
	{{ ref('internal_streams') }}

```
| result |
| :--- |
| 0.94 |


#### 6. Given the stream duration (start and end time) and the movie duration, how many users watched at least 50% of any movie in the last week of the month (7 days)?
```sql
{{ config(
    materialized = 'table',
    database = 'iceberg',
    schema='trusted'
) }}


SELECT 
    COUNT(DISTINCT user_email) users_over_half_duration 
FROM 
    {{ ref('internal_streams') }}
INNER JOIN
    {{ ref('internal_movies') }}
    ON internal_streams.movie_title = internal_movies.title
WHERE
    cast(concat(substring(internal_streams.start_at, 1, 10),' ', substring(internal_streams.start_at, 12, 8)) AS TIMESTAMP) >= TIMESTAMP '2021-12-24 23:59:59' 
    AND date_diff('minute', cast(concat(substring(internal_streams.start_at, 1, 10),' ', substring(internal_streams.start_at, 12, 8)) AS TIMESTAMP), cast(concat(substring(internal_streams.end_at, 1, 10),' ', substring(internal_streams.end_at, 12, 8)) AS TIMESTAMP)) >= internal_movies.duration_mins / 2
```
| result |
| :--- |
| 712 |


All questions can also be answered directly by the following dbt models:

1. moves_based_on_books
2. users_affected
3. movies_based_on_books_by_singaporeans
4. avarage_streaming_duration
5. median_streaming_size
6. users_watched_50_perc


## Next Steps


Building a data platform for production requires careful consideration of various technologies and tools to ensure reliability, scalability, and maintainability. 

Here's a detailed plan for building the pipeline using Minio, Trino, dbt, Airflow, Great Expectations, Amundsen, Slack, PagerDuty, CI/CD pipelines, and dbt tests.

#### Data Stack:

`Minio`: Minio provides scalable object storage, making it suitable for storing large volumes of data. It's chosen for its simplicity, scalability, and compatibility with S3 APIs.

`Trino` (formerly PrestoSQL): Trino is used as the query engine for fast and interactive data analytics. It excels in handling complex queries across multiple data sources, providing high performance and flexibility.

`dbt (Data Build Tool)`: dbt is chosen for its ability to transform data in the warehouse and for its support of data modeling, version control, and testing. 

`Airflow`: Apache Airflow is selected as the workflow scheduler and orchestrator. Its DAG-based architecture enables easy management and scheduling of complex data pipelines. It provides features like task dependencies, retries, and monitoring, making it ideal for orchestrating ETL workflows.

`Great Expectations`: Great Expectations is a open source tool used for data quality validation. It allows for the definition of data quality rules and validation of data against those rules, ensuring the integrity and reliability of the data.

`Slack and PagerDuty`: Slack and PagerDuty integrations are added for real-time notifications and alerts. This ensures that any issues or failures in the data pipeline are quickly detected and addressed, minimizing downtime and maximizing data reliability.

`CI/CD Pipelines`: CI/CD pipelines are implemented for automated testing, deployment, and monitoring of data applications. This ensures that changes to the data pipeline are thoroughly tested and deployed in a controlled manner, reducing the risk of errors and downtime.

`Metadata Management`: Amundsen Data Catalog is integrated into the pipeline to provide metadata management and discovery capabilities. It allows users to search, explore, and understand data assets across the organization.

#### Reasoning:

`Scalability`: Minio and Trino are chosen for their scalability, allowing the platform to handle large volumes of data and complex queries efficiently.

`Flexibility`: Trino's support for querying various data sources and dbt's flexibility in data transformation make the platform adaptable to changing business requirements and diverse data sources.

`Reliability`: Airflow's task monitoring, retries, and error handling mechanisms ensure reliable execution of ETL workflows, while Great Expectations' data quality checks enhance the reliability of the data.

`Real-time Monitoring`: Slack and PagerDuty integrations provide real-time alerts and notifications, enabling proactive monitoring and quick response to any issues or failures in the data pipeline.

`Automation`: CI/CD pipelines automate testing, deployment, and monitoring, streamlining the development and deployment process and reducing manual effort and errors.

By combining these technologies and tools, the data platform can efficiently ingest, process, transform, and validate data, ensuring its integrity, reliability, and accessibility for downstream analytics and decision-making.
