with ranked_zones AS (
    SELECT 
    pickup_zone, 
    SUM(revenue_monthly_total_amount) as sum_total_amount, 
    RANK() OVER (ORDER BY SUM(revenue_monthly_total_amount) DESC) as zone_rank
    FROM {{ref('monthly_revenue_per_location')}}
    WHERE extract(year from revenue_month) = 2020
    AND service_type = 'Green'
    GROUP BY pickup_zone
)
SELECT pickup_zone, sum_total_amount
FROM ranked_zones
WHERE zone_rank = 1