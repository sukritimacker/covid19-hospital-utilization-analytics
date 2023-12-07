from etl_gov_to_gcs import etl_gov_to_gcs
from etl_gcs_to_gbq import etl_gcs_to_gbq
# from start_pyspark_jobs import start_pyspark_jobs
from prefect.deployments import Deployment
from typing import List

deployments: List[Deployment] = [
    Deployment.build_from_flow(
        flow=etl_gov_to_gcs,
        name="etl_gov_to_gcs",
    ),
    Deployment.build_from_flow(
        flow=etl_gcs_to_gbq,
        name="etl_gcs_to_gbq",
    ),
    # Deployment.build_from_flow(
    #     flow=start_pyspark_jobs,
    #     name="start_pyspark_jobs",
    # ),
]


if __name__ == "__main__":
    for deployment in deployments:
        deployment.apply()
