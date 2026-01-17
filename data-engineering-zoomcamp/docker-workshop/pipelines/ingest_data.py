import pandas as pd

# parameters
year = 2021
month = 1

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url =   f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

pg_user ='root'
pg_psw='root'
pg_host='localhost'
pg_port=5432
pg_db='ny_taxy'

# # Read a sample of the data
# prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
# df = pd.read_csv(prefix + 'yellow_tripdata_2021-01.csv.gz', nrows=100)
# in this way i am not able to telle pandas what type of data it is so we will ifxx it in the next
# code section!





df = pd.read_csv(
    url',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)
df.head()




from sqlalchemy import create_engine
# this will connect to the db running on port 5432 docker or not
engine = create_engine(f'postgresql://{pg_user}:{pg_psw}@{pg_host}:{pg_port}/{pg_db}')




# this will show basically the schema the sql alchemy is going to input in psql
print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[32]:


# what i want so far is just create a table no insert data so i do df.head(0) and it will take
# the columns names
# is going to return 0 for success
df.head(0).to_sql(name='yello_taxy_data', con=engine, if_exists='replace')


# In[33]:


# is not possible to insert all the df once it's to long and we do not know if is going to be ok
# so the solution is to divide it into chunks!!
df_iterator = pd.read_csv(url,
                          dtype = dtype,
                          parse_dates=parse_dates,
                          iterator=True,
                          chunksize=100000
                         )


# In[34]:


from tqdm.auto import tqdm


# In[35]:


for df_chunk in tqdm(df_iterator):
    df_chunk.to_sql(name='yello_taxy_data', con=engine, if_exists='append')


# In[ ]:




