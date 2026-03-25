"""@bruin
name: load.income_quintile
type: python
connection: gcp_conn
depends:
  - ingestion.income_quintile
materialization:
  type: table

@bruin"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import pandas as pd
from utils import GCS_BUCKET, gcs_storage_options


def materialize(**kwargs):
    df = pd.read_parquet(f"{GCS_BUCKET}/income_quintile.parquet", storage_options=gcs_storage_options())
    exclude = ['freq', 'age', 'sex', 'unit', 'country']
    id_cols = [c for c in df.columns if c in exclude]
    year_cols = [c for c in df.columns if c not in id_cols]

    # must be done here since BQ doesn't support pivoting with dinamic column names
    df = df.melt(id_vars=id_cols, value_vars=year_cols, var_name='year', value_name='income_quintile')
    df = df[df['sex'].isin(['F', 'M'])]
    df = df.dropna(subset=['income_quintile'])
    df['year'] = df['year'].str.replace('_', '', regex=False).astype(int)
    return df[['country', 'year', 'sex', 'income_quintile']]
