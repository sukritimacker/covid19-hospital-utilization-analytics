import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket, DataFrameSerializationFormat
from typing import List
import numpy as np


@task(
    description="fetch the dataset from the healthdata.gov",
    log_prints=True,
    retries=5,
)
def fetch_dataset() -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Started downloading the dataset")
    gov_url = "https://healthdata.gov/api/views/uqq2-txqb/rows.csv?accessType=DOWNLOAD&api_foundry=true"
    df = pd.read_csv(f"{gov_url}")
    logger.info(f"Successfully downloaded the dataset")
    return df


@task(description="store the dataframe in the data lake", log_prints=True)
def write_to_gcs(df: pd.DataFrame) -> None:
    gcp_block = GcsBucket.load("data-lake")
    gcp_block.upload_from_dataframe(
        df,
        to_path=f"raw_data.parquet",
        serialization_format=DataFrameSerializationFormat.PARQUET,
    )


@flow(
    description="child flow for extracting, transforming and loading the dataset from healthdata.gov into the data lake in gcs"
)
def etl_gov_to_gcs() -> None:
    raw_df = fetch_dataset()
    write_to_gcs(raw_df)


if __name__ == "__main__":
    etl_gov_to_gcs()
