"""@bruin
name: ingestion.trips
type: python
image: python:3.11

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd

def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    months = pd.date_range(start=start_date, end=end_date, freq="MS")

    frames = []
    for taxi_type in taxi_types:
        for month in months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month.year}-{month.month:02d}.parquet"
            try:
                df = pd.read_parquet(url)
                df["taxi_type"] = taxi_type
                frames.append(df)
            except Exception as e:
                print(f"Skipping {url}: {e}")

    final_dataframe = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return final_dataframe