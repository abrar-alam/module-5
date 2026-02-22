"""@bruin
type: python

image: python:3.11

connection: duckdb-default
@bruin"""

import os
import json

start_date_str = os.environ.get('BRUIN_START_DATE')
end_date_str = os.environ.get('BRUIN_END_DATE')
bruin_vars_str = os.environ.get('BRUIN_VARS', '{}')
bruin_vars = json.loads(bruin_vars_str)
taxi_types = bruin_vars.get('taxi_types')

print("start_date_str:", start_date_str)
print("end_date_str:", end_date_str)
print("taxi_types:", taxi_types)