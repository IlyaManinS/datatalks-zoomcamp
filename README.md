# Docker/Terraform chapter

<h3>Question 3</h3>

SELECT COUNT(*)  
FROM public.green_taxi_trips  
WHERE lpep_pickup_datetime BETWEEN '2025-11-01 00:00:00' AND '2025-11-30 23:59:59'  
AND trip_distance <= 1  

Answer: <b>8007</b>

<h3> Question 4 </h3>

-- using a CTE (can be done with the subqueries as well) in case there are multiple days with the same trip distance

WITH ranked_days AS  
(  
SELECT DATE(lpep_pickup_datetime) as date_pickup, MAX(trip_distance) as max_distance,  
RANK() OVER (ORDER BY MAX(trip_distance) DESC) as trip_rank  
FROM public.green_taxi_trips  
GROUP BY date_pickup HAVING MAX(trip_distance) < 100  
)  
SELECT date_pickup, max_distance  
FROM ranked_days   
WHERE trip_rank = 1  

Answer: <b>2025-11-14</b>

<h3> Question 5 </h3>

-- again, using a CTE in case there are multiple zones with the same total

WITH ranked_zones AS  
(  
SELECT "Zone", ROUND(SUM(total_amount)::numeric,2) as zone_total,  
RANK() OVER (ORDER BY SUM(total_amount) DESC) as zone_rank  
FROM public.green_taxi_trips t  
INNER JOIN public.taxi_zones z  
ON t."PULocationID" = z."LocationID"   
WHERE lpep_pickup_datetime BETWEEN '2025-11-18 00:00:00' AND '2025-11-18 23:59:59'  
GROUP BY "Zone"  
)  
SELECT "Zone", zone_total  
FROM ranked_zones  
WHERE zone_rank = 1 


Answer: <b>East Harlem North</b>


<h3> Question 6 </h3>

-- again, using a CTE in case there are multiple zones with the same maximum tip  

WITH ranked_drop_offs AS    
(  
SELECT dropoff."Zone", MAX(tip_amount) as max_amount,  
RANK() OVER (ORDER BY MAX(tip_amount) DESC) as zone_rank  
FROM public.taxi_zones pickup  
INNER JOIN public.green_taxi_trips t   
ON t."PULocationID" = pickup."LocationID"  
INNER JOIN public.taxi_zones dropoff    
ON t."DOLocationID" = dropoff."LocationID"   
WHERE pickup."Zone" = 'East Harlem North'  
AND lpep_pickup_datetime BETWEEN '2025-11-01 00:00:00' AND '2025-11-30 23:59:59'  
GROUP BY dropoff."Zone"  
)  
SELECT "Zone", max_amount  
FROM ranked_drop_offs r  
WHERE zone_rank = 1  

Answer: <b>Yorkville West</b>


# Workflow Orchestration Chapter (Kestra)

<h3>Question 1</h3>  

The information can be accessible via:  
Executions >> Execution details >> Metrics >> Name columns >> file.size = 134,481,400 bytes or <b>128.3 MiB</b>  

<h3>Question 2</h3>  

With the code we implement: 
```file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"``` , the rendered value will be <b>green_tripdata_2020-04.csv</b>  

<h3>Question 3</h3>  

The information can be accessible in BigQuery >> yellow_tripdata >> Details >> Storage Info >> Number of Rows  
Or you can run this query in your BigQuery:  

SELECT COUNT(*)  
FROM `your_project.zoomcamp.yellow_tripdata`   
WHERE filename like 'yellow_tripdata_2020%'  

Answer: <b>24,648,499</b>  

<h3>Question 4</h3>  

The information can be accessible in BigQuery >> green_tripdata >> Details >> Storage Info >> Number of Rows  
Or you can run this query in your BigQuery:  

SELECT COUNT(*)  
FROM `your_project.zoomcamp.yellow_tripdata`   
WHERE filename like 'yellow_tripdata_2020%'  

Answer: <b>1,734,051</b>  

<h3>Question 5</h3>  

The information can be accessible in BigQuery >> yellow_tripdata_2021_03 >> Details >> Storage Info >> Number of Rows  
Or you can run this query in your BigQuery:  

SELECT COUNT(*)  
FROM `your_project.zoomcamp.yellow_tripdata`   
WHERE filename = 'yellow_tripdata_2020_03.csv'  

Answer: <b>1,925,152</b>  

<h3>Question 6</h3>  

It can be done in the cron job expression:  
```
- id: yellow_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 10 1 * *"
    timezone: America/New_York
    inputs:
      taxi: yellow
```
Answer: <b>America/New_York</b>
