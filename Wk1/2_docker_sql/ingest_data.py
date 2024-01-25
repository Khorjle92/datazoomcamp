import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db= params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Use pandas to read the compressed CSV file
    df_iter = pd.read_csv(csv_name, compression='gzip', iterator = True, chunksize = 100000)
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
  

    df.to_sql(name=table_name,con=engine, if_exists='append')
        
        # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    while True:
        df = next(df_iter)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
    

        df.to_sql(name=table_name,con=engine, if_exists='append')
        print('chunk in')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description= "Ingest CSV to postgre")
    parser.add_argument('user', help = 'user')
    parser.add_argument('password', help = 'pw')
    parser.add_argument('host', help = 'host')
    parser.add_argument('port', help = 'port')
    parser.add_argument('db', help = 'db')
    parser.add_argument('table_name', help = 'tablename')
    parser.add_argument('url', help = 'url')

    args= parser.parse_args()


    main(args)