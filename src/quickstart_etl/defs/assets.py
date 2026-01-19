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
