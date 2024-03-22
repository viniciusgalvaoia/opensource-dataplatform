"""
internal:
- movies
- stream
- users

vendor:
- author
- books
- reviews
"""

import os
import uuid
import tempfile
import pyarrow as pa
import pandas as pd
import pyarrow.parquet as pq

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error


load_dotenv()


class MinioStorage(object):
    """
    This class is used to write data into the MinIO server
    """

    def __init__(self, endpoint=None, access_key=None, secret_key=None, bucket_name=None):
        """
        Initialize the class with the provided parameters.

        Args:
            endpoint (str or None, optional): The endpoint URL for connecting to the storage service. Defaults to None.
            access_key (str or None, optional): The access key for authentication. Defaults to None.
            secret_key (str or None, optional): The secret key for authentication. Defaults to None.
            bucket_name (str or None, optional): The name of the bucket to be used for the storage service. Defaults to None.
        """ 
        self.bucket_name = None
        self.client = None
        self.get_config_storage(endpoint, access_key, secret_key, bucket_name)

    def get_config_storage(self, endpoint, access_key, secret_key, bucket_name):
        """
        Get the configuration storage for the given endpoint, access key, secret key, and bucket name.

        Args:
            endpoint (str, optional): The endpoint URL for the Minio server. If not provided, it will be fetched from the environment variable "ENDPOINT".
            access_key (str, optional): The access key for the Minio server. If not provided, it will be fetched from the environment variable "ACCESS_KEY".
            secret_key (str, optional): The secret key for the Minio server. If not provided, it will be fetched from the environment variable "SECRET_KEY".
            bucket_name (str, optional): The name of the bucket to be used for storing the configuration. If not provided, it will be fetched from the environment variable "LANDING_BUCKET".

        Returns:
            None
        """
        endpoint = endpoint or os.getenv("ENDPOINT")
        access_key = access_key or os.getenv("ACCESS_KEY")
        secret_key = secret_key or os.getenv("SECRET_KEY")

        self.bucket_name = bucket_name or os.getenv("LANDING_BUCKET")
        self.client = Minio(endpoint, access_key, secret_key, secure=False)

    @staticmethod
    def create_dataframe(df, ds_type, format_type):
        """
        Converts data into a DataFrame or Table based on the provided parameters.

        Args:
            df: An object representing the data to be converted into a DataFrame.
            ds_type (str): A string representing the type of the data source ("vendor" or "internal").
            format_type (str): A string representing the desired format of the output.

        Returns:
            tuple: A tuple containing the created DataFrame or Table and the data source type.

        Converts data into a DataFrame or Table based on the provided parameters.
        """

        if format_type == "json":
            json_data = df.to_json(orient="records").encode('utf-8')
            return json_data, ds_type
        elif format_type == "parquet":
            parquet_table = pa.Table.from_pandas(df)
            return parquet_table, ds_type

    def create_object_name(self, file_prefix, object_name, format_type, timestamp):
        """
        Constructs the object name by concatenating input parameters using forward slashes.

        Args:
            file_prefix (str): The prefix for the file or object name.
            object_name (str): The name of the object.
            format_type (str): The format type or extension of the object.
            timestamp (str): The timestamp to be appended to the object name.

        Returns:
            str: The constructed object name.

        Constructs the object name by concatenating input parameters using forward slashes.
        """
        return f"{file_prefix}/{object_name}/{format_type}/{timestamp}"

    def upload_data(self, data, object_name, ds_type, format_type):
        """
        Uploads data to an S3 bucket based on the provided parameters.

        Args:
            data: The data to be uploaded.
            object_name (str): The name of the object in the S3 bucket.
            ds_type (str): The type of the data source.
            format_type (str): The format type of the data.

        Returns:
            str: The result of the data upload.
        """

        file_loc_root_folder: str = "strider"
        file_prefix = file_loc_root_folder + "/" + ds_type

        if format_type == "parquet":

            file_uuid = str(uuid.uuid4())
            object_name = self.create_object_name(file_prefix, object_name, format_type, file_uuid)
            try:
                with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
                    pq.write_table(data, temp_file.name)
                    temp_file.seek(0)
                    put_data = self.client.put_object(
                        bucket_name="landing",
                        object_name=f"{object_name}.parquet",
                        data=temp_file,
                        length=os.path.getsize(temp_file.name),
                        content_type='application/octet-stream'
                    )
                    return put_data

            except S3Error as exc:
                print(f"error occurred while uploading data, {exc}")

    def write_file(self, ds_type: str, format_type: str):
        """
        Write data to a file based on the specified data source type and format type.

        Args:
            ds_type (str): The type of data source. Valid values are "internal", or "vendor".
            format_type (str): The type of format to save the data.

        Returns:
            tuple: If the data source type is "internal", returns a tuple containing the upload results for the "users", "streams and "movies" objects.
                   If the data source type is "vendor", returns a tuple containing the upload results for the "authors", "books", and "reviews" objects.
        """

        if ds_type == "internal":
            movies_df = pd.read_csv("src/sources/internal/movies.csv")
            streams_df = pd.read_csv("src/sources/internal/streams.csv")
            users_df = pd.read_csv("src/sources/internal/users.csv")

            movies_data, ds_type = self.create_dataframe(df=movies_df, ds_type=ds_type, format_type=format_type)
            streams_data, ds_type = self.create_dataframe(df=streams_df, ds_type=ds_type, format_type=format_type)
            users_data, ds_type = self.create_dataframe(df=users_df, ds_type=ds_type, format_type=format_type)

            return_movies = self.upload_data(data=movies_data, object_name="movies", ds_type=ds_type, format_type=format_type)
            return_streams = self.upload_data(data=streams_data, object_name="streams", ds_type=ds_type, format_type=format_type)
            return_users = self.upload_data(data=users_data, object_name="users", ds_type=ds_type, format_type=format_type)

            return return_movies, return_streams, return_users

        elif ds_type == "vendor":
            authors_df = pd.read_json("src/sources/vendor/authors.json", orient='records')
            books_df = pd.read_json("src/sources/vendor/books.json", orient='records')
            reviews_df = pd.read_json("src/sources/vendor/reviews.json", orient='records')

            authors_data, ds_type = self.create_dataframe(df=authors_df, ds_type=ds_type, format_type=format_type)
            books_data, ds_type = self.create_dataframe(df=books_df, ds_type=ds_type, format_type=format_type)
            reviews_data, ds_type = self.create_dataframe(df=reviews_df, ds_type=ds_type, format_type=format_type)

            return_authors = self.upload_data(data=authors_data, object_name="authors", ds_type=ds_type, format_type=format_type)
            return_books = self.upload_data(data=books_data, object_name="books", ds_type=ds_type, format_type=format_type)
            return_reviews = self.upload_data(data=reviews_data, object_name="reviews", ds_type=ds_type, format_type=format_type)

            return return_authors, return_books, return_reviews

