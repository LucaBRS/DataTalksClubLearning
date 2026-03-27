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

DATA_PATH = "/workspace/data/datalake"

def materialize(**kwargs):
    df = pd.read_parquet(f"{DATA_PATH}/income_quintile.parquet")
    exclude = ['freq', 'age', 'sex', 'unit', 'country']
    id_cols = [c for c in df.columns if c in exclude]
    year_cols = [c for c in df.columns if c not in id_cols]
    df = df.melt(id_vars=id_cols, value_vars=year_cols, var_name='year', value_name='income_quintile')
    df = df[df['sex'].isin(['F', 'M'])]
    df = df.dropna(subset=['income_quintile'])
    df['year'] = df['year'].str.replace('_', '', regex=False).astype(int)
    return df[['country', 'year', 'sex', 'income_quintile']].reset_index(drop=True)
