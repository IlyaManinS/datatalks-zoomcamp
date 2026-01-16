# docker-workshop
<h3>Question 3</h3>

SELECT COUNT(*)
FROM public.green_taxi_trips
WHERE lpep_pickup_datetime BETWEEN '2025-11-01 00:00:00' AND '2025-11-30 23:59:59'
AND trip_distance <= 1

8007

<h3> Question 4 </h3>

-- in case there are multiple days with the same trip distance

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
;

2025-11-14

<h3> Question 5 </h3>

-- again, in case there are multiple zones with the same total

WITH ranked_zones AS
(
SELECT "Zone", ROUND(SUM(total_amount)::numeric,2) as zone_total,
RANK() OVER (ORDER BY SUM(total_amount) DESC) as zone_rank
FROM public.green_taxi_trips t
INNER JOIN public.taxi_zones z
ON t."PULocationID" = z."LocationID"
GROUP BY "Zone"
)
SELECT "Zone", zone_total
FROM ranked_zones
WHERE zone_rank = 1
;

East Harlem North


<h3> Question 6 </h3>

-- this is the most "readable" version where we don't know the ID of the east harlem north zone. 
-- you can previously run the query against the name of the location and have one CTE with PULocationID = {id}
-- again, in case there are multiple zones with the same maximum tip


WITH east_harlem_north_trips AS
(
SELECT t.*
FROM public.green_taxi_trips t
INNER JOIN public.taxi_zones z
ON t."PULocationID" = z."LocationID"
WHERE "Zone" = 'East Harlem North'
)
, ranked_drop_offs AS
(
SELECT "Zone", MAX(tip_amount) as max_amount,
RANK() OVER (ORDER BY MAX(tip_amount) DESC) as zone_rank
FROM east_harlem_north_trips t
INNER JOIN public.taxi_zones z
ON t."DOLocationID" = z."LocationID"
GROUP BY "Zone"
)
SELECT "Zone", max_amount
FROM ranked_drop_offs r
WHERE zone_rank = 1
;

Yorkville West