from dagster import define_asset_job, AssetSelection
from dagster_pipeline.assets import github_raw_data, kaggle_raw_data, processed_data, data_quality_check

# Define the job for the data insights pipeline using asset selection instead of GraphDefinition
data_insights_job = define_asset_job(
    name="data_insights_job",
    selection=AssetSelection.assets(github_raw_data, kaggle_raw_data, processed_data, data_quality_check)
)