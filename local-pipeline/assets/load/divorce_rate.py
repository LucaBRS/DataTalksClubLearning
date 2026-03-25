"""@bruin
name: load.divorce_rate
type: python
connection: main_db
depends:
  - ingestion.divorce_rate
materialization:
  type: table
@bruin"""

import pandas as pd


def materialize(**kwargs):
    return pd.read_parquet("/workspace/data/datalake/divorce_rate.parquet")
