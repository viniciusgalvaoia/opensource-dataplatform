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

        :param endpoint: The endpoint URL for connecting to the storage service. Defaults to None.
        :type endpoint: str or None

        :param access_key: The access key for authentication. Defaults to None.
        :type access_key: str or None

        :param secret_key: The secret key for authentication. Defaults to None.
        :type secret_key: str or None

        :param bucket_name: The name of the bucket to be used for the storage service. Defaults to None.
        :type bucket_name: str or None

        """

        self.bucket_name = None
        self.client = None
        self.get_config_storage(endpoint, access_key, secret_key, bucket_name)

    def get_config_storage(self, endpoint, access_key, secret_key, bucket_name):
        """
        Get the configuration storage for the given endpoint, access key, secret key, and bucket name.

        :param endpoint: The endpoint URL for the Minio server. If not provided, it will be fetched from the environment variable "ENDPOINT".
        :param access_key: The access key for the Minio server. If not provided, it will be fetched from the environment variable "ACCESS_KEY".
        :param secret_key: The secret key for the Minio server. If not provided, it will be fetched from the environment variable "SECRET_KEY".
        :param bucket_name: The name of the bucket to be used for storing the configuration. If not provided, it will be fetched from the environment variable "LANDING_BUCKET".

        :return: None
        """

        endpoint = endpoint or os.getenv("ENDPOINT")
        access_key = access_key or os.getenv("ACCESS_KEY")
        secret_key = secret_key or os.getenv("SECRET_KEY")

        self.bucket_name = bucket_name or os.getenv("LANDING_BUCKET")
        self.client = Minio(endpoint, access_key, secret_key, secure=False)

    @staticmethod
    def create_dataframe(df, ds_type, format_type):
        """
        :param dt: a list or array-like object representing the data to be converted into a DataFrame
        :param ds_type: a string representing the type of the data source (vendor or internal)
        :param format_type: a string representing the desired format of the output
        :param is_cpf: a boolean indicating whether to include the 'cpf' column in the DataFrame (default: False)
        :return: a tuple containing the created DataFrame or Table and the data source type

        This method takes in the specified parameters and creates a DataFrame using the provided data.
        It then adds two additional columns: 'user_id' and 'dt_current_timestamp', which are generated
        using the 'api.gen_user_id()' and 'api.gen_timestamp()' methods respectively.

        If the 'is_cpf' parameter is set to True, an additional column 'cpf' is added to the DataFrame.
        The 'cpf' column is generated using the 'api.gen_cpf()' method.

        If the 'format_type' parameter is set to "json" and the 'ds_type' parameter is not "redis", the DataFrame is
        converted to JSON format using the 'to_json()' method and encoded as UTF*-8.
        The encoded JSON data and the 'ds_type' are returned as a tuple.

        If the 'format_type' parameter is not "json", the DataFrame is converted to a Parquet table
        using the 'pa.Table.from_pandas()' method. The Parquet table and the 'ds_type' are returned * as a tuple.
        """

        if format_type == "json":
            json_data = df.to_json(orient="records").encode('utf-8')
            return json_data, ds_type
        elif format_type == "parquet":
            parquet_table = pa.Table.from_pandas(df)
            return parquet_table, ds_type

    def create_object_name(self, file_prefix, object_name, format_type, timestamp):
        """
        :param file_prefix: The prefix for the file or object name.
        :param object_name: The name of the object.
        :param format_type: The format type or extension of the object.
        :param timestamp: The timestamp to be appended to the object name.
        :return: The object name concatenating the input parameters using forward slashes.
        """

        return f"{file_prefix}/{object_name}/{format_type}/{timestamp}"

    def upload_data(self, data, object_name, ds_type, format_type):
        """
        :param data: The data to be uploaded.
        :param object_name: The name of the object in the S3 bucket.
        :param ds_type: The type of the data source.
        :param format_type: The format type of the data (json or parquet).
        :return: The result of the data upload.

        Uploads data to an S3 bucket based on the provided parameters. If the format type is "json",
        the data is uploaded as a json file. If the format type is "parquet", the data is uploaded * as a parquet file.
        """

        file_loc_root_folder: str = "strider"
        file_prefix = file_loc_root_folder + "/" + ds_type

        if format_type == "parquet":

            file_uuid = str(uuid.uuid4())
            object_name = self.create_object_name(file_prefix, object_name, format_type, file_uuid)

            print(f"file location: {object_name}")

            try:
                with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:

                    print(f"data: {data}")
                    print(f"datatype: {type(data)}")
                    print(f"temp_file.name : {temp_file.name}")
                    pq.write_table(data, temp_file.name)
                    temp_file.seek(0)
                    print("Inserindo o dado")
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

        :param ds_type: The type of data source. Valid values are "mssql", "postgres", "mongodb", "redis".
        :param format_type: The type of format to save the data.
        :return: If the data source type is "mssql", returns a tuple containing the upload results for the "users" and "credit_card" objects.
                 If the data source type is "postgres", returns a tuple containing the upload results for the "payments", "subscription", and "vehicle" objects.
                 If the data source type is "mongodb", returns a tuple containing the upload results for the "rides", "users", and "stripe" objects.
                 If the data source type is "redis", returns a tuple containing the upload results for the "google_auth", "linkedin_auth", and "apple_auth" objects.
        """

        if ds_type == "internal":
            movies_df = pd.read_csv("src/sources/internal/movies.csv")
            print(movies_df.head())
            streams_df = pd.read_csv("src/sources/internal/streams.csv")
            users_df = pd.read_csv("src/sources/internal/users.csv")

            print("Terminou de ler os dados internal")

            movies_data, ds_type = self.create_dataframe(df=movies_df, ds_type=ds_type, format_type=format_type)
            streams_data, ds_type = self.create_dataframe(df=streams_df, ds_type=ds_type, format_type=format_type)
            users_data, ds_type = self.create_dataframe(df=users_df, ds_type=ds_type, format_type=format_type)

            print("dataframes criados internal")

            print(movies_data)

            return_movies = self.upload_data(data=movies_data, object_name="movies", ds_type=ds_type, format_type=format_type)
            return_streams = self.upload_data(data=streams_data, object_name="streams", ds_type=ds_type, format_type=format_type)
            return_users = self.upload_data(data=users_data, object_name="users", ds_type=ds_type, format_type=format_type)

            return return_movies, return_streams, return_users

        elif ds_type == "vendor":
            authors_df = pd.read_json("src/sources/vendor/authors.json", orient='records')
            print(authors_df.head())
            books_df = pd.read_json("src/sources/vendor/books.json", orient='records')
            reviews_df = pd.read_json("src/sources/vendor/reviews.json", orient='records')

            print("Terminou de ler os dados vendor")

            authors_data, ds_type = self.create_dataframe(df=authors_df, ds_type=ds_type, format_type=format_type)
            books_data, ds_type = self.create_dataframe(df=books_df, ds_type=ds_type, format_type=format_type)
            reviews_data, ds_type = self.create_dataframe(df=reviews_df, ds_type=ds_type, format_type=format_type)
            
            print("dataframes criados vendor")


            return_authors = self.upload_data(data=authors_data, object_name="authors", ds_type=ds_type, format_type=format_type)
            return_books = self.upload_data(data=books_data, object_name="books", ds_type=ds_type, format_type=format_type)
            return_reviews = self.upload_data(data=reviews_data, object_name="reviews", ds_type=ds_type, format_type=format_type)

            return return_authors, return_books, return_reviews

