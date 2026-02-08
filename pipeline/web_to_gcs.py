import io
import os
import requests
import pandas as pd
from google.cloud import storage

init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "warehouse-project-zoomcamp-ilya")

# Number of rows per chunk — keeps memory usage low
# when processing large files like yellow taxi data
CHUNK_SIZE = 100_000


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):

        # sets the month part of the file_name string
        month = '0' + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read csv in chunks and write each chunk as a separate parquet file
        # this avoids loading the entire file into memory at once,
        # which can cause OOM kills on large files (e.g. yellow taxi data)
        parquet_base = file_name.replace('.csv.gz', '')
        chunk_files = []

        for j, chunk in enumerate(pd.read_csv(file_name, compression='gzip', low_memory=False, chunksize=CHUNK_SIZE)):
            chunk_parquet = f"{parquet_base}_part{j:03d}.parquet"
            chunk.to_parquet(chunk_parquet, engine='pyarrow')
            chunk_files.append(chunk_parquet)

        print(f"Parquet: {len(chunk_files)} chunk(s) created")

        # upload each chunk to GCS, then clean up local files
        for chunk_parquet in chunk_files:
            upload_to_gcs(BUCKET, f"{service}/{chunk_parquet}", chunk_parquet)
            os.remove(chunk_parquet)  # free up disk space after upload

        # remove the original csv.gz to free disk space
        os.remove(file_name)

        print(f"GCS: {service}/{parquet_base}_part*.parquet")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')