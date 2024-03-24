from dagster_dbt import DbtCliResource, dbt_assets
from .constants import dbt_manifest_path
from dagster import (
    asset,
    AssetExecutionContext,
    AssetObservation,
    Output,
    DailyPartitionsDefinition,
    EnvVar,
)
from .resources import UpBankResource, MinIOResource, DuckDBResource
import pandas as pd
import json
import arrow

PARTITION_START_DATE = "2023-12-01"
BUCKET_NAME = EnvVar("S3_FINANCE_DATA_BUCKET").get_value()


@asset(
    compute_kind="python",
    group_name="collection",
    metadata={"partition_expr": "date"},
    partitions_def=DailyPartitionsDefinition(
        start_date=PARTITION_START_DATE,
        timezone="Australia/Melbourne",
    ),
)
def collect_up_transactions(
    context: AssetExecutionContext,
    up_client: UpBankResource,
    minio_client: MinIOResource,
) -> Output[None]:
    """Get Up transactions from the API into the data lake"""
    bounds = context.partition_time_window
    rs = up_client.list_transactions(since=bounds.start, until=bounds.end)
    metadata = None
    for r in rs:
        data = pd.DataFrame(r.json()["data"]).to_dict(orient="records")
        for _ in data:
            created_date = arrow.get(_["attributes"]["createdAt"])
            created_dt_utc = created_date.to("utc")
            object_name = f"transactions/up/{created_dt_utc.date()}/transactions_{created_dt_utc.date()}_{created_dt_utc.time()}.json"
            metadata = minio_client.write_to_minio(
                bucket_name=BUCKET_NAME,
                object_prefix=object_name,
                data=json.dumps(_).encode(),
                content_type="application/json",
            )
    return Output(value=None, metadata=metadata)


@asset(compute_kind="python", group_name="collection")
def collect_up_categories(
    up_client: UpBankResource, minio_client: MinIOResource
) -> Output[None]:
    """Get Up categories from the API into the data lake"""
    response = up_client.list_categories()
    data = response.json()["data"]
    metadata = None
    for _ in data:
        category_id = _["id"]
        object_name = f"categories/up/{arrow.utcnow().date()}/{category_id}.json"
        metadata = minio_client.write_to_minio(
            bucket_name=BUCKET_NAME,
            object_prefix=object_name,
            data=json.dumps(_).encode(),
            content_type="application/json",
        )
    return Output(value=None, metadata=metadata)


@asset(compute_kind="python", group_name="collection")
def collect_up_accounts(
    up_client: UpBankResource, minio_client: MinIOResource
) -> Output[None]:
    """Get Up categories from the API into the data lake"""
    accounts = up_client.list_accounts()
    metadata = None
    for account in accounts:
        payload = account.json()
        for data in payload["data"]:
            account_id = data["id"]
            object_name = f"accounts/up/{arrow.utcnow().date()}/{account_id}.json"
            metadata = minio_client.write_to_minio(
                bucket_name=BUCKET_NAME,
                object_prefix=object_name,
                data=json.dumps(data).encode(),
                content_type="application/json",
            )
    return Output(value=None, metadata=metadata)


@asset(
    compute_kind="duckdb",
    deps=[collect_up_transactions],
    key_prefix="raw",
    group_name="ingestion",
)
def raw_up_transactions(duckdb_client: DuckDBResource):
    """Ingest Up transactions in DuckDB for further processing"""
    statement = f"""
    SELECT
        *,
        get_current_timestamp() as _loaded_timestamp
    FROM read_json_auto('s3://{BUCKET_NAME}/transactions/up/**/*.json')
    """
    return duckdb_client.create_table(statement, "raw_up_transactions")


@asset(
    compute_kind="duckdb",
    deps=[collect_up_categories],
    key_prefix="raw",
    group_name="ingestion",
)
def raw_up_categories(duckdb_client: DuckDBResource):
    """Ingest Up categories in DuckDB for further processing"""
    statement = f"""
        SELECT
            *,
            get_current_timestamp() as _loaded_timestamp
        FROM read_json_auto('s3://{BUCKET_NAME}/categories/up/**/*.json')
        """
    return duckdb_client.create_table(statement, "raw_up_categories")


@asset(
    compute_kind="duckdb",
    deps=[collect_up_accounts],
    key_prefix="raw",
    group_name="ingestion",
)
def raw_up_accounts(duckdb_client: DuckDBResource):
    """Ingest Up accounts in DuckDB for further processing"""
    statement = f"""
            SELECT
                *,
                get_current_timestamp() as _loaded_timestamp
            FROM read_json_auto('s3://{BUCKET_NAME}/accounts/up/**/*.json')
            """
    return duckdb_client.create_table(statement, "raw_up_accounts")


@dbt_assets(manifest=dbt_manifest_path)
def finance_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
