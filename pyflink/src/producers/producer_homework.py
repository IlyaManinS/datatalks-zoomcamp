import dataclasses                                                                                                       
import json                                                                                                            
import time                                                                                                              
                                                                                                                        
import pandas as pd                                                                                                      
from kafka import KafkaProducer                                                                                        
from dataclasses import dataclass


@dataclass
class Ride:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float


def ride_from_row(row):                                                                                                
    return Ride(                                                                                                         
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count']) if pd.notna(row['passenger_count']) else 0,
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')


url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 
'trip_distance', 'tip_amount', 'total_amount']
df = pd.read_parquet(url, columns=columns)

server = 'localhost:9092'
topic_name = 'green-trips'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

t0 = time.time()

for _, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    # time.sleep(0.01)

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')