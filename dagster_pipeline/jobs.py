from dagster import job, GraphDefinition

from dagster_pipeline.assets import github_raw_data, kaggle_raw_data, processed_data, data_quality_check

# Define the job for the data insights pipeline
data_insights_job = GraphDefinition(
    node_defs=[
        github_raw_data,
        kaggle_raw_data,
        processed_data,
        data_quality_check
    ],
    name="data_insights_job",
).to_job()