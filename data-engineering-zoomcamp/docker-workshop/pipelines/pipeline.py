import sys
import pandas as pd


print(f"arguments: {sys.argv}")

month = int(sys.argv[1])

df = pd.DataFrame({"A":[1,2],"B":[3,4]})
df['month'] = month
print(df.head())

df.to_parquet('test.parquet')


print(f'hello pipeline, month: {month} ')