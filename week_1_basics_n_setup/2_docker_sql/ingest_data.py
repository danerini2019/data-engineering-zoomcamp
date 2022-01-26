import pandas as pd
import time
from sqlalchemy import create_engine

# importing csv to show schema of table

# df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

# df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

# print(pd.io.sql.get_schema(df, name="yellow_taxi_data", con=engine))

# importing data by chunks

df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)

# df = next(df_iter)

# df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
# df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

# start = time.time()
# df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')
# end = time.time()
# print(end - start)

while True:
    start = time.time()

    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.to_sql(name='yellow_taxi_data', con=engine, if_exists=
    'append')

    end = time.time()

    print('insert another chunk..., took %.3f seconds' % (end - start))

