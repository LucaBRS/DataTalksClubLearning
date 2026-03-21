


"""@bruin
name: ingestion.marriage_rate
connection: local_duckdb

materialization:
  type: table
@bruin"""

# /// script
# dependencies = [
#   "eurostat",
#   "pandas",
# ]
# ///

import eurostat
import pandas as pd


def materialize(**kwargs):
    df = eurostat.get_data_df("tps00206")

    # df = df.rename(columns={
    #     "geo\\TIME_PERIOD": "country",
    # })

    # df = df.melt(
    #     id_vars=["freq", "unit", "country"],
    #     var_name="year",
    #     value_name="marriage_rate"
    # )

    # df["year"] = df["year"].astype(int)
    # df = df.drop(columns=["freq", "unit"])
    # df = df.dropna(subset=["marriage_rate"])

    return df

print(materialize().head())