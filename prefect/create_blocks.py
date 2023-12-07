from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
import os
import json

with open("service_account_creds.json") as f:
    contents = f.read()

with open("data_lake_bucket_name.txt") as f:
    bucket_name = f.read()
    bucket_name = bucket_name.replace("\n", "")

service_account_info = json.loads(contents)

GcpCredentials(service_account_info=service_account_info).save(
    "service-account", overwrite=True
)

GcsBucket(
    gcp_credentials=GcpCredentials.load("service-account"),
    bucket=bucket_name,
).save("data-lake", overwrite=True)
