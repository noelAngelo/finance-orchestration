from dagster import ConfigurableResource, MetadataValue
from pydantic import Field
from contextlib import contextmanager
from dagster._utils.backoff import backoff
import requests
import io
from minio import Minio
from collections.abc import Iterator
import duckdb
import pandas as pd


class UpBankResource(ConfigurableResource):
    """Resource for interacting with Up Bank API.

    Examples:
        . code-block:: python


    """

    pat: str = Field(
        description=(
            "Personal Access Token that allows you to access Up Bank's API and retrieve some of your personal data."
        )
    )
    api_version: str = Field(description="The API version", default="/api/v1")
    api_host: str = Field(
        description="The host as to where all the requests are to be made",
        default="https://api.up.com.au",
    )
    page_size: int = Field(
        description="The number of transactions to be returned in a single query",
        default=None,
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @contextmanager
    def get_connection(self) -> Iterator:
        conn = backoff(fn=requests.Session, retry_on=(RuntimeError,), max_retries=10)
        conn.headers.update({"Authorization": f"Bearer {self.pat}"})
        yield conn

    def is_healthy(self) -> bool:
        """Make a basic ping request to the API. This is useful to verify that authentication is functioning
        correctly."""
        endpoint = "/util/ping"
        with self.get_connection(self) as s:
            r = s.get(f"{self.api_host}{self.api_version}{endpoint}")
            if r.status_code == 200:  # 200 OK
                return True
            raise NotImplementedError

    def list_categories(self) -> requests.Response:
        """Retrieve a list of all categories and their ancestry. The returned list is not paginated."""
        endpoint = "/categories"
        with self.get_connection() as s:
            r = s.get(f"{self.api_host}{self.api_version}{endpoint}")
            if r.status_code == 200:  # 200 OK
                return r
            raise NotImplementedError

    def list_accounts(self) -> list[requests.Response]:
        """Accounts represent the underlying store used to track balances and the transactions that have occurred to
        modify those balances over time."""
        endpoint = "/accounts"
        responses = []
        with self.get_connection() as s:
            r = s.get(f"{self.api_host}{self.api_version}{endpoint}")
            if r.status_code == 200:  # 200 OK
                responses.append(r)
                next_url = r.json()["links"]["next"]
                while next_url:
                    next_r = s.get(next_url)
                    if next_r.status_code == 200:  # 200 OK
                        responses.append(next_r)
                        next_url = next_r.json()["links"]["next"]
                return responses
            raise NotImplementedError

    def list_transactions(self, **kwargs) -> list[requests.Response]:
        endpoint = "/transactions"
        params = {f"filter[{k}]": v for k, v in kwargs.items()}
        if self.page_size:
            params["page[size]"] = self.page_size
        responses = []
        with self.get_connection() as s:
            r = s.get(url=f"{self.api_host}{self.api_version}{endpoint}", params=params)
            if r.status_code == 200:  # 200 OK
                responses.append(r)
                next_url = r.json()["links"]["next"]
                while next_url:
                    next_r = s.get(next_url)
                    if next_r.status_code == 200:  # 200 OK
                        responses.append(next_r)
                        next_url = next_r.json()["links"]["next"]
                return responses
            raise NotImplementedError


class MinIOResource(ConfigurableResource):
    """Resource for interacting with the MinIO S3 API

    Examples:
        . code-block:: python

    """

    s3_host: str = Field(default="localhost")
    s3_port: str = Field(default="9000")
    s3_access_key_id: str = Field(default="minio")
    s3_secret_access_key: str = Field(default="minio123")
    s3_use_ssl: str = Field(default="False")

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @contextmanager
    def get_connection(self):
        conn = backoff(
            fn=Minio,
            kwargs={
                "endpoint": f"{self.s3_host}:{self.s3_port}",
                "access_key": self.s3_access_key_id,
                "secret_key": self.s3_secret_access_key,
                "secure": False,
                "cert_check": False,
            },
            retry_on=(RuntimeError,),
        )

        yield conn

    def write_to_minio(
        self,
        bucket_name: str,
        object_prefix: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict:
        with self.get_connection() as client:

            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)

            client.put_object(
                bucket_name=bucket_name,
                object_name=object_prefix,
                data=io.BytesIO(data),
                content_type=content_type,
                length=len(data),
            )
            object_path = "/".join(object_prefix.split("/")[:-1])
            return {
                "bucket_name": bucket_name,
                "object_path": object_path,
                "s3_path": MetadataValue.path(f"s3://{bucket_name}/{object_prefix}"),
                "size (bytes)": len(data),
            }


class DuckDBResource(ConfigurableResource):
    """Resource for interacting with DuckDB"""

    path: str = Field(description="File path of the DuckDB database")
    packages: list = Field(description="List of packages to install")
    options: dict = Field(description="DuckDB configuration")

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @contextmanager
    def get_connection(self):
        conn = backoff(
            fn=duckdb.connect,
            retry_on=(RuntimeError,),
            max_retries=10,
            kwargs={"database": self.path},
        )
        yield conn

    def query(self, statement, db) -> pd.DataFrame:
        for package in self.packages:
            query = f"install '{package}'; load '{package}';"
            db.execute(query)
            print(query)
        for option, val in self.options.items():
            query = f"SET {option}='{val}';"
            db.execute(query)
            print(query)
        results = db.execute(statement).fetch_df()
        return results

    def create_table(self, statement: str, table_name: str):
        with self.get_connection() as db:
            for package in self.packages:
                query = f"install '{package}'; load '{package}';"
                db.execute(query)
                print(query)
            for option, val in self.options.items():
                query = f"SET {option}='{val}';"
                db.execute(query)
                print(query)
            create_statement = f"CREATE OR REPLACE TABLE {table_name} AS ({statement});"
            db.execute(create_statement)
            db.commit()
