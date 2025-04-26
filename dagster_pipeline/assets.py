from dagster import asset, AssetIn, Output
import os
import sys

# Add parent directory to path so we can import our scrapers and processors
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.github_scraper import GitHubScraper
from scrapers.kaggle_scraper import KaggleScraper
from processors.data_processor import DataProcessor


@asset
def github_raw_data():
    """Extract data from GitHub repositories related to data science."""
    scraper = GitHubScraper(limit=25)
    repos = scraper.scrape_repositories()
    return Output(
        value=len(repos),
        metadata={
            "num_repos": len(repos),
            "preview": str(repos[:2]) if repos else "[]",
            "data_path": scraper.raw_data_path
        }
    )


@asset
def kaggle_raw_data():
    """Extract data from Kaggle datasets related to data science."""
    scraper = KaggleScraper(limit=25)
    datasets = scraper.scrape_datasets()
    return Output(
        value=len(datasets),
        metadata={
            "num_datasets": len(datasets),
            "preview": str(datasets[:2]) if datasets else "[]",
            "data_path": scraper.raw_data_path
        }
    )


@asset(
    ins={
        "github_data": AssetIn("github_raw_data"),
        "kaggle_data": AssetIn("kaggle_raw_data")
    }
)
def processed_data(github_data, kaggle_data):
    """Process and combine GitHub and Kaggle data."""
    # Check if we have data from both sources
    if github_data == 0 or kaggle_data == 0:
        raise Exception("Missing data from one or both sources")
    
    processor = DataProcessor()
    output_path = processor.process_data()
    
    return Output(
        value=output_path,
        metadata={
            "output_path": output_path,
            "github_count": github_data,
            "kaggle_count": kaggle_data,
        }
    )


@asset(
    ins={"processed_data_path": AssetIn("processed_data")}
)
def data_quality_check(processed_data_path):
    """Perform data quality checks on the processed data."""
    import pandas as pd
    import glob
    
    # Get all CSV files in the processed directory
    csv_files = glob.glob(f"{processed_data_path}/*.csv")
    
    quality_results = {}
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        
        try:
            # Load the CSV
            df = pd.read_csv(file_path)
            
            # Check for empty dataframe
            if df.empty:
                quality_results[file_name] = "FAIL: Empty dataframe"
                continue
            
            # Check for missing values
            missing_pct = df.isna().sum().sum() / (df.shape[0] * df.shape[1]) * 100
            
            if missing_pct > 20:
                quality_results[file_name] = f"WARNING: High missing values ({missing_pct:.2f}%)"
            else:
                quality_results[file_name] = f"PASS: Missing values ({missing_pct:.2f}%)"
            
        except Exception as e:
            quality_results[file_name] = f"ERROR: {str(e)}"
    
    return Output(
        value=quality_results,
        metadata={
            "quality_results": quality_results,
            "files_checked": len(csv_files)
        }
    )