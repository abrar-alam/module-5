"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips
type: python
image: python:3.11

# TODO: Set the connection.
# I commented out (AA)
connection: duckdb-default 

materialization:
  type: table
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: integer
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: integer
  - name: trip_distance
    type: double
  - name: ratecode_id
    type: integer
  - name: store_and_fwd_flag
    type: string
  - name: pulocation_id
    type: integer
  - name: dolocation_id
    type: integer
  - name: payment_type
    type: integer
  - name: fare_amount
    type: double
  - name: extra
    type: double
  - name: mta_tax
    type: double
  - name: tip_amount
    type: double
  - name: tolls_amount
    type: double
  - name: improvement_surcharge
    type: double
  - name: total_amount
    type: double
  - name: congestion_surcharge
    type: double
  - name: airport_fee
    type: double
  - name: taxi_type
    type: string
  - name: extracted_at
    type: timestamp

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import pandas as pd
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
    start_date_str = os.getenv('BRUIN_START_DATE')
    end_date_str = os.getenv('BRUIN_END_DATE')
    vars_json = os.getenv('BRUIN_VARS')
    
    if vars_json:
        vars_dict = json.loads(vars_json)
        taxi_types = vars_dict.get('taxi_types', ['yellow'])
    else:
        taxi_types = ['yellow']
    
    # Generate list of months between start and end
    start = datetime.fromisoformat(start_date_str)
    end = datetime.fromisoformat(end_date_str)
    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append(current)
        # Move to next month
        next_month = current.month % 12 + 1
        next_year = current.year + (current.month // 12)
        current = current.replace(year=next_year, month=next_month, day=1)
    
    dfs = []
    for taxi_type in taxi_types:
        for month in months:
            year = month.year
            month_str = f"{month.month:02d}"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month_str}.parquet"
            try:
                df = pd.read_parquet(url)
                # Standardize datetime column names
                if taxi_type in ['yellow', 'green']:
                    prefix = 'tpep_' if taxi_type == 'yellow' else 'lpep_'
                    df.rename(columns={
                        f'{prefix}pickup_datetime': 'pickup_datetime',
                        f'{prefix}dropoff_datetime': 'dropoff_datetime'
                    }, inplace=True)
                # For fhv, already pickup_datetime, dropoff_datetime
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.now()
                dfs.append(df)
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
                continue
    
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()


