import json
import os

GCS_BUCKET = os.environ["GCS_BUCKET"]


def gcs_storage_options() -> dict:
    return {"token": json.loads(os.environ["GCP_CREDENTIALS"])}
