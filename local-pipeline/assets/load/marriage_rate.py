"""@bruin
name: load.marriage_rate
type: python
connection: main_db
depends:
  - ingestion.marriage_rate
materialization:
  type: table
@bruin"""

import pandas as pd


def materialize(**kwargs):
    return pd.read_parquet("/workspace/data/datalake/marriage_rate.parquet")
