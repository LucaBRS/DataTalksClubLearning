import eurostat
import os

DATA_PATH = "/workspace/data/datalake"


def ingest_eurostat(dataset_code: str, output_name: str) -> None:
    df = eurostat.get_data_df(dataset_code)
    df = df.rename(columns={"geo\\TIME_PERIOD": "country"})
    os.makedirs(DATA_PATH, exist_ok=True)
    df.to_parquet(f"{DATA_PATH}/{output_name}.parquet", index=False)
