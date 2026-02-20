"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import pandas as pd
import requests
import os
import json
from datetime import datetime, timedelta

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # Get environment variables
    start_date_str = os.environ.get('BRUIN_START_DATE')
    end_date_str = os.environ.get('BRUIN_END_DATE')
    bruin_vars_str = os.environ.get('BRUIN_VARS', '{}')
    bruin_vars = json.loads(bruin_vars_str)
    taxi_types = bruin_vars.get('taxi_types', ['yellow'])  # Default to yellow if not specified

    # Parse dates
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

    # Generate list of months between start and end
    months = []
    current = start_date.replace(day=1)
    while current <= end_date:
        months.append(current.strftime('%Y-%m'))
        current += timedelta(days=32)
        current = current.replace(day=1)

    # Fetch data
    dfs = []
    for taxi_type in taxi_types:
        for month in months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month}.parquet"
            try:
                response = requests.get(url)
                response.raise_for_status()
                df = pd.read_parquet(pd.io.common.BytesIO(response.content))
                dfs.append(df)
            except requests.RequestException as e:
                print(f"Failed to fetch {url}: {e}")
                continue

    # Concatenate DataFrames
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
    else:
        # Return empty DataFrame with expected columns if no data
        final_df = pd.DataFrame(columns=[
            'vendor_id', 'pickup_datetime', 'dropoff_datetime', 'passenger_count',
            'trip_distance', 'rate_code_id', 'store_and_fwd_flag', 'pu_location_id',
            'do_location_id', 'payment_type', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
            'congestion_surcharge', 'airport_fee', 'extracted_at'
        ])

    # Add extracted_at column
    final_df['extracted_at'] = pd.Timestamp.now()

    return final_df


