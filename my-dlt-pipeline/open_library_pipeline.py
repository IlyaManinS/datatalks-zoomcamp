"""`dlt` pipeline ingesting data from the Open Library Books REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source() -> None:
    """Define dlt resources for the Open Library Books API."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    "path": "api/books",
                    # Example request for a couple of identifiers; adjust as needed.
                    "params": {
                        # The API supports multiple identifiers separated by commas.
                        # See: https://openlibrary.org/dev/docs/api/books
                        "bibkeys": "ISBN:0201558025,LCCN:93005405",
                        "format": "json",
                        "jscmd": "data",
                    },
                    # Select all book records from the top-level object keyed by bibkey.
                    "data_selector": "$.*",
                },
            },
        ],
        # set `resource_defaults` to apply configuration to all endpoints
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
