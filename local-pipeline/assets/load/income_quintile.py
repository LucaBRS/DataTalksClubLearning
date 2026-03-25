"""@bruin
name: load.income_quintile
type: python
connection: main_db
depends:
  - ingestion.income_quintile
materialization:
  type: table
@bruin"""

import pandas as pd


def materialize(**kwargs):
    return pd.read_parquet("/workspace/data/datalake/income_quintile.parquet")
