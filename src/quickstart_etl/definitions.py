from dagster import Definitions
from dagster_poc.assets import pipeline_status
from dagster_poc.sensors import pipeline_run_monitor

defs = Definitions(
    assets=[pipeline_status],
    sensors=[pipeline_run_monitor],
)
