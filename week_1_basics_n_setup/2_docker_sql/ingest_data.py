import pandas as pd
pd.__version__

df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))
