from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import os
import json

with open("service_account_creds.json") as f:
    contents = f.read()

bucket_name = os.getenv("DATA_LAKE_BUCKET")

service_account_info = json.loads(contents)

GcpCredentials(service_account_info=service_account_info).save(
    "service-account", overwrite=True
)

GcsBucket(
    gcp_credentials=GcpCredentials.load("service-account"),
    bucket=bucket_name,
).save("data-lake", overwrite=True)
