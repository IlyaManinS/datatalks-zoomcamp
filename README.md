# docker-workshop
DataTalks Workshop for Docker

<!-- docker run -it ubuntu - to access docker ubuntu -->

<!-- apt update - to refresh the docker local with the machine packages -->

<!-- apt install -y python3 python3-pip - to install python8 -->

<!-- docker run -it python:3.13.11 - to run it with python (unless, needs to be installed every time) -->
<!-- docker run -it python:3.13.11-slim to install the lighter version -->

<!-- bash input -->
docker run -it --entrypoint=bash python:3.13.11-slim

<!-- to see images -->
docker ps -a

<!-- build based on the dockerfile -->
docker build -t test:pandas .

<!-- run it afterwards -->
docker run -it --entrypoint=bash --rm test:pandas 12

<!-- This is for Postgres, rm means the information won't be saved -->

docker run -it --rm \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  <!-- -v ny_taxi_postgres_data:/var/lib/postgresql \ -- optional -->
  -p 5432:5432 \
  postgres:18

<!-- uv run postgres -->
uv run pgcli -h localhost -p 5432 -u root -d ny_taxi


<!-- Script to ingest -->
uv run python ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1 \
  --chunksize=5000

<!-- to create a network -->
docker network create pg-network

<!-- to view networks -->
docker network ls 

<!-- to delete networks -->
docker network rm "ID"

<!-- Now, run the docker for 5432 port -->

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -p 5432:5432 \
  --network=pg-network \
  --name pgdatabase \
  postgres:18

  <!-- Now, run pg admin -->

  docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  <!-- -v pgadmin_data:/var/lib/pgadmin \ -- volume is optional too -->
  -p 8085:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4

<!-- After updating the dockerfile, use this to ingest the data with Docker -->

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --pg-user=root \
    --pg-pass=root \
    --pg-host=pgdatabase \
    --pg-port=5432 \
    --pg-db=ny_taxi \
    --target-table=yellow_taxi_trips_2021_2 \
    --year=2021 \
    --month=2 \
    --chunksize=100000


