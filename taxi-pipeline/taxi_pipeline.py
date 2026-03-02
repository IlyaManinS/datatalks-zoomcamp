"""NYC Taxi REST API -> DuckDB using dlt.

Run:
    python taxi_pipeline.py
"""

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

