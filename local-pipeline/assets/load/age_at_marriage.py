"""@bruin
name: load.age_at_marriage
type: python
connection: main_db
depends:
  - ingestion.age_at_marriage
materialization:
  type: table
@bruin"""

import pandas as pd


def materialize(**kwargs):
    return pd.read_parquet("/workspace/data/datalake/age_at_marriage.parquet")
