from dagster import Definitions
from dagster_pipeline.assets import github_raw_data, kaggle_raw_data, processed_data, data_quality_check
from dagster_pipeline.schedules import daily_schedule, weekly_schedule
from dagster_pipeline.jobs import data_insights_job
from dagster_pipeline.schedules import on_demand_sensor

defs = Definitions(
    assets=[github_raw_data, kaggle_raw_data, processed_data, data_quality_check],
    schedules=[daily_schedule, weekly_schedule],
    jobs=[data_insights_job],
    sensors=[on_demand_sensor],
)