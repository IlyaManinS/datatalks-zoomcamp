SELECT SUM(total_monthly_trips) as sum_total_monthly_trips
FROM {{ref('monthly_revenue_per_location')}}
WHERE revenue_month = '2019-10-01'
AND service_type = 'Green'

