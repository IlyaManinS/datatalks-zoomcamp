import pandas as pd
from sqlalchemy import create_engine
import click


@click.command()
@click.option('--pg-user', default = 'root', help = 'PostgreSQL user')
@click.option('--pg-pass', default = 'root', help = 'PosgreSQL password')
@click.option('--pg-host', default = 'localhost', help = 'PosgreSQL host')
@click.option('--pg-port', default = 5432, help = 'PostgreSQL port')
@click.option('--pg-db', default = 'ny_taxi', help = 'PostgreSQL database name')
@click.option('--target-table', default = 'taxi_zones', help = 'PostgreSQL table name')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table):
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    print(f'Downloading from {url}')

    df = pd.read_csv(url)

    df.to_sql(
            name=target_table, 
            con=engine, 
            index = False,
            if_exists='replace'
            )

    print(f'Ingestion completed, loaded {len(df)} rows')

if __name__ == '__main__':
    run()
