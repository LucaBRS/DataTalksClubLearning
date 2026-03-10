import requests
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
from math import ceil
import os


url_green = os.getenv("GREEN_TAXY_URL")
if not url_green:
    raise RuntimeError("GREEN_TAXY_URL is not set")


url_zones = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"


location_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

r = requests.get(url_green, headers={"User-Agent": "Mozilla/5.0"})
r.raise_for_status()

if len(r.content) == 0:
    raise RuntimeError("Empty response (likely WAF challenge)")

df_g = pd.read_parquet(url_green)
df_z = pd.read_csv(url_zones, dtype=location_dtype)

pg_user = os.getenv("PGUSER", "root")
pg_pass = os.getenv("PGPASSWORD", "root")

pg_host = os.getenv("PGHOST", "postgres")  # IMPORTANT: service name in compose
pg_port = int(os.getenv("PGPORT", "5432"))
pg_db = os.getenv("PGDATABASE", "ny_taxi")
# year = 2021
# month = 1
target_table_1 = "green_taxi_data"
target_table_2 = "taxi_zone_data"
chunksize = 100000

engine = create_engine(f"postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")


def chunker(df, size):
    for i in range(0, len(df), size):
        yield df.iloc[i : i + size]


def iteretor_data(df, target_table):

    total_chunks = ceil(len(df) / chunksize)
    first = True

    for df_chunk in tqdm(chunker(df, chunksize), total=total_chunks):
        if first:
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace")
            first = False

        df_chunk.to_sql(name=target_table, con=engine, if_exists="append")


if __name__ == "__main__":
    iteretor_data(df_g, target_table_1)
    iteretor_data(df_z, target_table_2)
