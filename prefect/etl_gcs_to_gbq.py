import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket, GcpCredentials
from typing import List
from io import BytesIO


@task(
    description="fetch the parquet for the year provided from the data lake",
    log_prints=True,
    retries=5,
)
def extract_from_gcs() -> bytes:
    logger = get_run_logger()
    gcs_block = GcsBucket.load("data-lake")
    raw_df = gcs_block.read_path(f"raw_data.parquet")
    logger.info(f"Successfully downloaded raw_ata.parquet file")
    return raw_df



@task(description="append the dataframe to the data warehouse", log_prints=True)
def write_to_gbq(df: pd.DataFrame) -> None:
    logger = get_run_logger()
    creds_block = GcpCredentials.load("service-account")
    df.to_gbq(
        destination_table="raw_dataset.raw_data",
        project_id=creds_block.project,
        chunksize=1_000,
        if_exists="append",
        credentials=creds_block.get_credentials_from_service_account(),
        progress_bar=False
    )
    logger.info(f"Successfully added the dataframe to gbq")
    return df


@flow(
    description="child flow for extracting, transforming and loading the dataset from gcs into the data warehouse in gbq"
)
def etl_gcs_to_gbq() -> None:
    raw_df = extract_from_gcs()
    df = pd.read_parquet(BytesIO(raw_df))
    write_to_gbq(df)

if __name__ == "__main__":
    etl_gcs_to_gbq()
