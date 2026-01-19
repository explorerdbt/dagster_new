from dagster import Definitions

from quickstart_etl.defs.monitoring_asset import (
    monitored_pipeline,
    long_running_alert_sensor,
)

defs = Definitions(
    assets=[monitored_pipeline],
    sensors=[long_running_alert_sensor],
)
