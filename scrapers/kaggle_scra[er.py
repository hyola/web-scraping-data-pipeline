import os
import json
import time
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi
from dotenv import load_dotenv

load_dotenv()

class KaggleScraper:
    def __init__(self, limit=100, search_tags=None):
        """
        Initialize Kaggle scraper with API.
        
        Args:
            limit: Maximum number of datasets to scrape
            search_tags: List of tags to filter datasets
        """
        self.api = KaggleApi()
        self.api.authenticate()
        self.limit = limit
        self.search_tags = search_tags or ["data science", "machine learning", 
                                          "deep learning", "data analysis", 
                                          "artificial intelligence"]
        self.raw_data_path = "data/raw/kaggle_datasets.json"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.raw_data_path), exist_ok=True)
    
    def scrape_datasets(self):
        """Scrape Kaggle datasets related to data science."""
        all_datasets = []
        
        for tag in self.search_tags:
            print(f"Scraping datasets for tag: {tag}")
            try:
                # Search for datasets with the tag
                datasets_list = self.api.dataset_list(search=tag, sort_by="hottest")
                
                count = 0
                for dataset in datasets_list:
                    if count >= self.limit:
                        break
                    
                    # Add delay between requests to be respectful
                    time.sleep(1)
                    
                    try:
                        # Get dataset details
                        dataset_data = {
                            "ref": dataset.ref,
                            "title": dataset.title,
                            "url": f"https://www.kaggle.com/datasets/{dataset.ref}",
                            "description": dataset.description,
                            "total_bytes": dataset.totalBytes,
                            "size": self._format_size(dataset.totalBytes),
                            "download_count": dataset.downloadCount,
                            "view_count": dataset.viewCount,
                            "vote_count": dataset.voteCount,
                            "last_updated": dataset.lastUpdated,
                            "tags": dataset.tags,
                            "owner_name": dataset.ownerName,
                            "license_name": dataset.licenseName,
                            "file_count": len(dataset.files) if hasattr(dataset, 'files') else None
                        }
                        
                        # Check if dataset already exists in our list
                        if not any(d["ref"] == dataset_data["ref"] for d in all_datasets):
                            all_datasets.append(dataset_data)
                            count += 1
                            print(f"  Scraped {count}/{self.limit}: {dataset.ref}")
                        
                    except Exception as e:
                        print(f"  Error scraping {dataset.ref}: {str(e)}")
                
            except Exception as e:
                print(f"Error with tag {tag}: {str(e)}")
        
        # Save the scraped data
        with open(self.raw_data_path, 'w', encoding='utf-8') as f:
            json.dump(all_datasets, f, ensure_ascii=False, indent=2)
            
        print(f"Scraped {len(all_datasets)} datasets successfully!")
        return all_datasets
    
    def _format_size(self, size_bytes):
        """Format bytes to human-readable size."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"

if __name__ == "__main__":
    # Test the scraper
    scraper = KaggleScraper(limit=20)
    scraper.scrape_datasets()