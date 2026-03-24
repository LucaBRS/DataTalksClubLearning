"""@bruin
name: ingestion.age_at_marriage
connection: local_duckdb

materialization:
  type: table
@bruin"""

import eurostat
import pandas as pd


def materialize(**kwargs):
    df = eurostat.get_data_df("tps00014")
    df = df.rename(columns={"geo\\TIME_PERIOD": "country"})
    return df
