import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

class DataProcessor:
    def __init__(self):
        """Initialize data processor."""
        self.github_path = "data/raw/github_repos.json"
        self.kaggle_path = "data/raw/kaggle_datasets.json"
        self.processed_path = "data/processed/data_science_insights.csv"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.processed_path), exist_ok=True)
    
    def load_data(self):
        """Load data from raw JSON files."""
        try:
            with open(self.github_path, 'r', encoding='utf-8') as f:
                github_data = json.load(f)
            
            with open(self.kaggle_path, 'r', encoding='utf-8') as f:
                kaggle_data = json.load(f)
                
            return github_data, kaggle_data
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return [], []
    
    def process_github_data(self, github_data):
        """Process GitHub repository data."""
        if not github_data:
            return pd.DataFrame()
        
        # Convert to DataFrame
        github_df = pd.DataFrame(github_data)
        
        # Extract primary programming language percentages
        github_df['language_percentages'] = github_df['languages'].apply(
            lambda x: ", ".join([f"{lang}: {pct/sum(x.values())*100:.1f}%" 
                                 for lang, pct in x.items()]) if x else ""
        )
        
        # Extract main topics (up to 5)
        github_df['main_topics'] = github_df['topics'].apply(
            lambda x: ", ".join(x[:5]) if x else ""
        )
        
        # Calculate days since last update
        github_df['updated_at'] = pd.to_datetime(github_df['updated_at'])
        github_df['days_since_update'] = (
            pd.to_datetime('today') - github_df['updated_at']
        ).dt.days
        
        # Extract keywords from README
        github_df['readme_keywords'] = github_df['readme_content'].apply(self._extract_keywords)
        
        # Select and rename columns
        github_processed = github_df[[
            'name', 'full_name', 'description', 'url', 'stars', 'forks',
            'created_at', 'updated_at', 'days_since_update', 
            'primary_language', 'language_percentages', 'main_topics',
            'readme_keywords'
        ]].copy()
        
        github_processed.rename(columns={
            'name': 'repo_name',
            'full_name': 'repo_full_name', 
            'url': 'repo_url',
            'stars': 'repo_stars',
            'forks': 'repo_forks'
        }, inplace=True)
        
        return github_processed
    
    def process_kaggle_data(self, kaggle_data):
        """Process Kaggle dataset data."""
        if not kaggle_data:
            return pd.DataFrame()
        
        # Convert to DataFrame
        kaggle_df = pd.DataFrame(kaggle_data)
        
        # Convert updated date
        kaggle_df['last_updated'] = pd.to_datetime(kaggle_df['last_updated'])
        kaggle_df['days_since_update'] = (
            pd.to_datetime('today') - kaggle_df['last_updated']
        ).dt.days
        
        # Join tags into a string
        kaggle_df['dataset_tags'] = kaggle_df['tags'].apply(
            lambda x: ", ".join(x) if x else ""
        )
        
        # Calculate popularity score
        kaggle_df['popularity_score'] = (
            kaggle_df['download_count'] * 0.5 + 
            kaggle_df['view_count'] * 0.3 + 
            kaggle_df['vote_count'] * 0.2
        )
        
        # Select and rename columns
        kaggle_processed = kaggle_df[[
            'ref', 'title', 'url', 'description', 'size', 
            'download_count', 'view_count', 'vote_count',
            'popularity_score', 'last_updated', 'days_since_update', 
            'dataset_tags', 'owner_name', 'license_name'
        ]].copy()
        
        kaggle_processed.rename(columns={
            'ref': 'dataset_ref',
            'title': 'dataset_title',
            'url': 'dataset_url',
            'description': 'dataset_description'
        }, inplace=True)
        
        return kaggle_processed
    
    def find_relationships(self, github_df, kaggle_df):
        """
        Find potential relationships between GitHub repos and Kaggle datasets.
        This is a simple keyword-based matching approach.
        """
        # If either DataFrame is empty, return empty DataFrame
        if github_df.empty or kaggle_df.empty:
            return pd.DataFrame()
        
        # Create a relationship matrix
        relationships = []
        
        for _, repo in github_df.iterrows():
            repo_keywords = set(self._extract_text_keywords(
                f"{repo['repo_name']} {repo['description']} {repo['main_topics']} {repo['readme_keywords']}"
            ))
            
            for _, dataset in kaggle_df.iterrows():
                dataset_keywords = set(self._extract_text_keywords(
                    f"{dataset['dataset_title']} {dataset['dataset_description']} {dataset['dataset_tags']}"
                ))
                
                # Find common keywords
                common_keywords = repo_keywords.intersection(dataset_keywords)
                similarity_score = len(common_keywords) / max(len(repo_keywords), len(dataset_keywords), 1)
                
                # Only consider matches with some similarity
                if similarity_score > 0.1:
                    relationships.append({
                        'repo_name': repo['repo_name'],
                        'repo_url': repo['repo_url'],
                        'dataset_title': dataset['dataset_title'],
                        'dataset_url': dataset['dataset_url'],
                        'similarity_score': similarity_score,
                        'common_keywords': ', '.join(common_keywords)
                    })
        
        # Convert to DataFrame and sort by similarity
        rel_df = pd.DataFrame(relationships)
        if not rel_df.empty:
            rel_df = rel_df.sort_values('similarity_score', ascending=False)
        
        return rel_df
    
    def process_data(self):
        """Process all data and create final CSV."""
        # Load data
        github_data, kaggle_data = self.load_data()
        
        # Process individual data sources
        github_df = self.process_github_data(github_data)
        kaggle_df = self.process_kaggle_data(kaggle_data)
        
        # Find relationships between repos and datasets
        relationships_df = self.find_relationships(github_df, kaggle_df)
        
        # Prepare final datasets
        # 1. Top GitHub repositories
        top_github = github_df.sort_values('repo_stars', ascending=False).head(50)
        top_github['data_source'] = 'github'
        
        # 2. Top Kaggle datasets
        top_kaggle = kaggle_df.sort_values('popularity_score', ascending=False).head(50)
        top_kaggle['data_source'] = 'kaggle'
        
        # 3. Top relationships
        top_relationships = relationships_df.head(50)
        top_relationships['data_source'] = 'relationship'
        
        # Create metrics dataframe
        metrics = pd.DataFrame([{
            'total_github_repos': len(github_df),
            'total_kaggle_datasets': len(kaggle_df),
            'total_relationships_found': len(relationships_df),
            'avg_github_stars': github_df['repo_stars'].mean(),
            'avg_kaggle_downloads': kaggle_df['download_count'].mean(),
            'top_github_language': github_df['primary_language'].value_counts().index[0] if not github_df.empty else '',
            'top_kaggle_tag': self._most_common_tag(kaggle_df['dataset_tags']) if not kaggle_df.empty else '',
            'processed_date': datetime.now().strftime('%Y-%m-%d')
        }])
        metrics['data_source'] = 'metrics'
        
        # Save individual reports
        top_github.to_csv("data/processed/top_github_repos.csv", index=False)
        top_kaggle.to_csv("data/processed/top_kaggle_datasets.csv", index=False)
        relationships_df.to_csv("data/processed/dataset_repo_relationships.csv", index=False)
        metrics.to_csv("data/processed/metrics.csv", index=False)
        
        print("Individual reports saved successfully!")
        
        # Return main insights file path
        return "data/processed/"
    
    def _extract_keywords(self, text, max_keywords=10):
        """Extract key words from text content."""
        if not text or not isinstance(text, str):
            return ""
            
        # Very simple keyword extraction - in a real project, use NLP techniques
        # Remove common words, keep only significant terms
        common_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 
                       'for', 'with', 'by', 'about', 'as', 'of', 'that', 'this', 'is', 
                       'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
                       'do', 'does', 'did', 'will', 'would', 'should', 'can', 'could'}
        
        # Split by non-alphanumeric characters
        words = ''.join(c if c.isalnum() else ' ' for c in text.lower()).split()
        
        # Filter out common words and short words
        keywords = [w for w in words if w not in common_words and len(w) > 3]
        
        # Count frequency
        word_freq = {}
        for word in keywords:
            word_freq[word] = word_freq.get(word, 0) + 1
        
        # Get top keywords
        top_keywords = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
        return ", ".join(word for word, _ in top_keywords[:max_keywords])
    
    def _extract_text_keywords(self, text):
        """Extract keywords from text for matching."""
        if not text or not isinstance(text, str):
            return []
            
        # Remove non-alphanumeric characters and convert to lowercase
        clean_text = ''.join(c if c.isalnum() else ' ' for c in text.lower())
        
        # Split into words and filter out short words
        keywords = [word for word in clean_text.split() if len(word) > 3]
        
        return keywords
    
    def _most_common_tag(self, tag_series):
        """Find the most common tag across all datasets."""
        all_tags = []
        for tags_str in tag_series:
            if tags_str:
                all_tags.extend([tag.strip() for tag in tags_str.split(',')])
        
        if not all_tags:
            return ""
            
        # Count occurrences
        tag_counts = {}
        for tag in all_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
        # Find most common
        most_common = max(tag_counts.items(), key=lambda x: x[1])
        return most_common[0]


if __name__ == "__main__":
    # Test the processor
    processor = DataProcessor()
    processor.process_data()