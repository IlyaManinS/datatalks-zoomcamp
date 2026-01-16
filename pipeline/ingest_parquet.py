import pandas as pd
from sqlalchemy import create_engine
import click

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2025, type=int, help='Year of the data')
@click.option('--month', default=11, type=int, help='Month of the data')
@click.option('--chunksize', default=5000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='green_taxi_trips', help='Target table name')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    # Ingestion logic here
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    url = f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f'Downloading from {url}')

    df = pd.read_parquet(
        url,
        engine = 'pyarrow'
    )

    df.head(0).to_sql(
        name = target_table,
        con = engine,
        if_exists = 'replace',
        index = False
    )

    total_chunks = (len(df)+chunksize-1)//chunksize
    print(f'There will be {total_chunks} chunks of load')

    for i in range(0, len(df), chunksize):
        chunk = df.iloc[i:i+chunksize]
        chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append',
            index = False,
            chunksize = chunksize
            )
        print(f'Inserted chunk {i//chunksize + 1}/{total_chunks}')
    
    print(f'Ingestion completed, loaded {len(df)} rows')

if __name__ == '__main__':
    run()
