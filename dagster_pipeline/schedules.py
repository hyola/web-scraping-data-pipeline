from dagster import ScheduleDefinition, build_schedule_from_partitioned_job, RunRequest

from dagster_pipeline.jobs import data_insights_job

# Daily schedule that runs the pipeline once a day (for demonstration purposes)
daily_schedule = ScheduleDefinition(
    job=data_insights_job,
    cron_schedule="0 0 * * *",  # Run at midnight every day
    execution_timezone="UTC",
)

# Weekly schedule that runs the pipeline once a week (for demonstration purposes)
weekly_schedule = ScheduleDefinition(
    job=data_insights_job,
    cron_schedule="0 0 * * 0",  # Run at midnight on Sunday
    execution_timezone="UTC",
)

# Add a sensor for immediate/on-demand runs
from dagster import sensor, RunRequest, SensorEvaluationContext

@sensor(job=data_insights_job)
def on_demand_sensor(context: SensorEvaluationContext):
    """
    This sensor can be used to trigger runs on-demand for testing and demonstration.
    To use it, click the "Skip" button in the Dagit UI when viewing this sensor.
    """
    if context.last_run_key is None:
        # Only yield a run if we haven't already done so
        yield RunRequest(run_key="on_demand_run", run_config={})