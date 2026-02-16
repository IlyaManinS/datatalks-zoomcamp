with taxi_zone_lookup AS
(
    SELECT *
    FROM {{ref('taxi_zone_lookup')}}
)
, renamed AS(
    SELECT 
    locationID AS location_id,
    Borough,
    Zone,
    service_zone
    FROM taxi_zone_lookup
)
SELECT *
FROM renamed