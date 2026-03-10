# Module 5 Homework: PySpark

## Question 1: Install Spark and PySpark

**What is the output of `spark.version`?**

**Answer:** `4.1.1`

```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

print(f"Spark version: {spark.version}")
```

## Question 2: Average Parquet File Size

**What is the average size of the Parquet files that were created (in MB)?**

**Answer:** ~25MB

```python
df = spark.read \
    .option("header", "true") \
    .parquet('yellow_tripdata_2025-11.parquet')

df \
    .repartition(4) \
    .write.parquet('data/pq/yellow_tripdata_2025-11/')
```

```
$ ls -lh data/pq/yellow_tripdata_2025-11/
total 101M
-rw-r--r-- 1 vscode vscode 25M part-00000-...-c000.snappy.parquet
-rw-r--r-- 1 vscode vscode 25M part-00001-...-c000.snappy.parquet
-rw-r--r-- 1 vscode vscode 25M part-00002-...-c000.snappy.parquet
-rw-r--r-- 1 vscode vscode 25M part-00003-...-c000.snappy.parquet
```

## Question 3: Taxi Trips on November 15

**How many taxi trips were there on the 15th of November?**

**Answer:** 162,604

```python
from pyspark.sql import functions as F

df.filter(F.col('tpep_pickup_datetime') >= '2025-11-15') \
    .filter(F.col('tpep_pickup_datetime') < '2025-11-16') \
    .count()
```

## Question 4: Longest Trip

**What is the length of the longest trip in the dataset in hours?**

**Answer:** 90.6 hours

```python
df.withColumn(
    'trip_hours',
    (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600
) \
.agg(F.max('trip_hours').alias('longest_trip_hours')) \
.show()
```

```
+------------------+
|longest_trip_hours|
+------------------+
| 90.64666666666666|
+------------------+
```

## Question 5: Spark UI Port

**Spark's User Interface which shows the application's dashboard runs on which local port?**

**Answer:** 4040

## Question 6: Least Frequent Pickup Location

**What is the name of the least frequent pickup location Zone?**

**Answer:** Arden Heights

```python
df_zones = spark.read \
    .option("header", "true") \
    .parquet('taxi_zone_lookup.parquet')

df_zones = df_zones \
    .withColumn('LocationID', df_zones.LocationID.cast(T.IntegerType()))

df \
    .groupBy('PULocationID') \
    .agg(F.count('*').alias('pickup_count')) \
    .join(df_zones, df.PULocationID == df_zones.LocationID) \
    .orderBy('pickup_count', ascending=True) \
    .select('Zone', 'pickup_count') \
    .show(1)
```

```
+-------------+------------+
|         Zone|pickup_count|
+-------------+------------+
|Arden Heights|           1|
+-------------+------------+
```
