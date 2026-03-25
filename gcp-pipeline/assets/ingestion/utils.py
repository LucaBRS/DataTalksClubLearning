import eurostat
import json
import os

GCS_BUCKET = os.environ["GCS_BUCKET"]


def ingest_eurostat(dataset_code: str, output_name: str) -> None:
    print(f"Downloading {dataset_code} from Eurostat...")
    df = eurostat.get_data_df(dataset_code)
    df = df.rename(columns={"geo\\TIME_PERIOD": "country"})
    print(f"Downloaded {len(df)} rows, saving to GCS...")
    credentials = json.loads(os.environ["GCP_CREDENTIALS"])
    df.to_parquet(
        f"{GCS_BUCKET}/{output_name}.parquet",
        index=False,
        storage_options={"token": credentials},
    )
    print(f"Saved {output_name}.parquet to GCS.")
