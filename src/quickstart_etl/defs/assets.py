import random
import time
import pandas as pd
import plotly.express as px

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
)

@asset
def monitored_pipeline(context: AssetExecutionContext) -> Output:
    """
    A pipeline that:
    - randomly succeeds or fails
    - emits logs
    - emits charts inside Dagster UI
    """

    start_time = time.time()
    context.log.info("Pipeline started")

    # Simulate work
    time.sleep(random.randint(1, 5))

    # Random success / failure
    status = random.choice(["SUCCESS", "FAILURE"])

    if status == "FAILURE":
        context.log.error("Pipeline failed intentionally")
    else:
        context.log.info("Pipeline succeeded")

    duration = round(time.time() - start_time, 2)

    # Create dataframe for chart
    df = pd.DataFrame({
        "metric": ["duration_seconds"],
        "value": [duration]
    })

    fig = px.bar(
        df,
        x="metric",
        y="value",
        title="Pipeline Run Duration"
    )

    return Output(
        status,
        metadata={
            "status": status,
            "duration_seconds": duration,
            "duration_chart": MetadataValue.plotly(fig),
        },
    )


from dagster import sensor, RunStatus, SkipReason

@sensor(run_status=RunStatus.SUCCESS)
def long_running_alert_sensor(context):
    """
    Triggers alert if pipeline runtime > 2 minutes
    """

    run = context.dagster_run
    if not run.start_time or not run.end_time:
        return SkipReason("Run timing info not available")

    runtime_seconds = run.end_time - run.start_time

    if runtime_seconds > 120:
        context.log.warning(
            f"ALERT: Run took {runtime_seconds} seconds (> 2 minutes)"
        )
    else:
        return SkipReason("Run finished within allowed time")
