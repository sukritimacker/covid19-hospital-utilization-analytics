import pandas as pd
from prefect import flow, task, get_run_logger
from prefect_gcp.cloud_storage import GcsBucket, GcpCredentials
from typing import List
import subprocess
import os


@task(log_prints=True)
def spark_submit(file: str):
    logger = get_run_logger()
    logger.info(f"Starting job: {file}")
    res = subprocess.run(
        [
            "gcloud",
            "dataproc",
            "jobs",
            "submit",
            "pyspark",
            file,
            f"--cluster={os.environ['CLUSTER']}",
            f"--region={os.environ['REGION']}",
            "--",
            os.getenv("DATAPROC_TEMP_BUCKET"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding='utf-8'
    )
    logger.info(res.stdout)
    logger.info(f"Job status code: {res.returncode}")


@flow()
def start_pyspark_jobs() -> None:
    logger = get_run_logger()
    pyspark_dir = os.path.join(os.path.expanduser("~"), "pyspark")
    jobs = sorted(os.listdir(pyspark_dir))
    logger.info("Running jobs in order:\n" + '\n'.join(jobs))
    for job in jobs:
        spark_submit(os.path.join(pyspark_dir, job))


if __name__ == "__main__":
    start_pyspark_jobs()
