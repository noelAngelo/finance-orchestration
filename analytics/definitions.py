import os

from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_dbt import DbtCliResource

from . import assets
from .constants import dbt_project_dir
from .schedules import schedules
from .resources import UpBankResource, MinIOResource, DuckDBResource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "duckdb_client": DuckDBResource(
            path=EnvVar("DUCKDB_PATH"),
            packages=["httpfs", "json", "postgres"],
            options=dict(s3_endpoint=EnvVar("S3_HOST").get_value()+":"+EnvVar("S3_PORT").get_value(),
                         s3_access_key_id=EnvVar("S3_ACCESS_KEY_ID").get_value(),
                         s3_secret_access_key=EnvVar("S3_SECRET_ACCESS_KEY").get_value(),
                         s3_use_ssl=EnvVar("S3_USE_SSL").get_value(),
                         s3_url_style="path")
        ),
        "up_client": UpBankResource(pat=EnvVar("UP_BANK_PAT")),
        "minio_client": MinIOResource(
            s3_host=EnvVar("S3_HOST"),
            s3_port=EnvVar("S3_PORT"),
            s3_access_key_id=EnvVar("S3_ACCESS_KEY_ID"),
            s3_secret_access_key=EnvVar("S3_SECRET_ACCESS_KEY"),
            s3_use_ssl=EnvVar("S3_USE_SSL"),
        ),
    },
)
