## NYC Taxi dlt REST API Pipeline

Short pipeline that ingests paginated NYC taxi trips from the Zoomcamp REST API into DuckDB using `dlt`, then answers a few basic analytics questions.

### Pipeline code

```python
import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api/"


@dlt.source
def nyc_taxi_rest_api_source():
    """Define dlt resources for the NYC Taxi REST API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": BASE_URL,
        },
        "resource_defaults": {
            # This API has no stable unique identifier in the payload, so `replace`
            # keeps the pipeline idempotent across repeated runs.
            "write_disposition": "replace",
        },
        "resources": [
            {
                "name": "nyc_taxi",
                "columns": {
                    # These fields are present in the API but may be null-only in the sample,
                    # so we declare them explicitly to ensure they are materialized.
                    "rate_code": {"data_type": "text"},
                    "mta_tax": {"data_type": "double"},
                },
                "endpoint": {
                    # The base URL itself returns the paginated dataset.
                    "path": "",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        # This API returns a JSON array (no "total" field), so we
                        # must disable total pages detection and stop on empty page.
                        "total_path": None,
                        "stop_after_empty_page": True,
                    },
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="taxi_data",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)  # noqa: T201
```

Data is stored in `taxi_pipeline.duckdb`, table `taxi_data.nyc_taxi`.

---

### Question 1 – Start and end date of the dataset

```sql
SELECT
  MIN(trip_pickup_date_time)  AS min_pickup,
  MAX(trip_dropoff_date_time) AS max_dropoff
FROM taxi_data.nyc_taxi;
```

**Answer**:  
- Start date: **2009-06-01** (min pickup `2009-06-01 11:33:00+00:00`)  
- End date: **2009-07-01** (max dropoff `2009-07-01 00:03:00+00:00`)

---

### Question 2 – Proportion of trips paid with credit card (pct)

```sql
SELECT
  100.0 * SUM(CASE WHEN payment_type ILIKE 'credit%' THEN 1 ELSE 0 END)
        / COUNT(*) AS pct_credit_card
FROM taxi_data.nyc_taxi;
```

**Answer**: **26.66%** of trips are paid with credit card.

---

### Question 3 – Total amount of money generated in tips (USD)

```sql
SELECT
  SUM(tip_amt) AS total_tips
FROM taxi_data.nyc_taxi;
```

**Answer**: **USD 6,063.41** total tips across all trips in the dataset.

---

### Taxi analytics dashboard (marimo + ibis + Altair)

This project also includes an interactive marimo dashboard in `taxi_dashboard_marimo.py` that connects to `taxi_pipeline.duckdb` via ibis and visualizes key metrics.

- **How to run**
  - Start the dashboard:
    - `marimo run taxi_dashboard_marimo.py` (simple run), or
    - `marimo edit taxi_dashboard_marimo.py` (open in browser editor).
  - The app reads from the `taxi_data.nyc_taxi` table produced by the `taxi_pipeline`.

- **Chart 1 – Daily passengers and revenue**
  - **Title**: `Daily passengers and total amount`
  - **Description**: Dual-axis line chart by pickup date.
    - X: `trip_date` (day of `trip_pickup_date_time`).
    - Left Y: Sum of `passenger_count` (blue line, “Total passengers”).
    - Right Y: Sum of `total_amt` in USD (red line, “Total amount (USD)”).
  - **Comment**: This shows how rider volume and revenue move together over the month and makes it easy to spot days with unusually high or low demand.

- **Chart 2 – Average trip distance per day**
  - **Title**: `Average trip distance per day`
  - **Description**: Vertical bar chart with hover tooltip rounded to two decimals.
    - X: `trip_date`.
    - Y: `AVG(trip_distance)` (average distance per trip).
  - **Comment**: Highlights whether longer trips cluster on specific days (for example weekends vs weekdays) and gives context for revenue per trip.

- **Chart 3 – Trip share by payment type**
  - **Title**: `Trip share by payment type`
  - **Description**: Pie chart with legend and percentage labels.
    - Slice size: `trips` (count of records per `payment_type`).
    - Legend: `payment_type` (CASH, Credit, etc.).
    - Labels: Percentage of total trips per payment type, shown only when they round to at least 0.1%.
  - **Comment**: Makes it easy to see that the majority of rides in this sample are paid in cash vs credit and how small “edge” categories like disputes or no-charge trips compare.

