"""@bruin
name: ingestion.income_quintile
connection: local_duckdb

materialization:
  type: table
@bruin"""

import eurostat
import pandas as pd


def materialize(**kwargs):
    df = eurostat.get_data_df("tessi180")
    df = df.rename(columns={"geo\\TIME_PERIOD": "country"})
    return df
