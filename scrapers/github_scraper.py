import os
import time
import json
from datetime import datetime
from github import Github
from github.GithubException import RateLimitExceededException
from dotenv import load_dotenv

load_dotenv()

class GitHubScraper:
    def __init__(self, topics=None, limit=100):
        """
        Initialize GitHub scraper with API token.
        
        Args:
            topics: List of data science related topics to search for
            limit: Maximum number of repositories to scrape per topic
        """
        self.token = os.getenv("GITHUB_TOKEN")
        self.g = Github(self.token)
        self.topics = topics or ["data-science", "machine-learning", "data-analysis", 
                                "deep-learning", "artificial-intelligence"]
        self.limit = limit
        self.raw_data_path = "data/raw/github_repos.json"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(self.raw_data_path), exist_ok=True)
    
    def _handle_rate_limit(self):
        """Handle GitHub API rate limiting by waiting until reset time."""
        rate_limit = self.g.get_rate_limit()
        reset_timestamp = rate_limit.core.reset.timestamp()
        sleep_time = reset_timestamp - datetime.now().timestamp() + 10  # Add 10s buffer
        
        if sleep_time > 0:
            print(f"Rate limit reached. Waiting for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
    
    def scrape_repositories(self):
        """Scrape GitHub repositories related to data science topics."""
        all_repos = []
        
        for topic in self.topics:
            print(f"Scraping repositories for topic: {topic}")
            try:
                # Search for repositories with the topic
                query = f"topic:{topic} stars:>10"
                repos = self.g.search_repositories(query=query)
                
                count = 0
                for repo in repos:
                    if count >= self.limit:
                        break
                    
                    # Add delay between requests to be respectful
                    time.sleep(2)
                    
                    try:
                        # Get repository languages
                        languages = repo.get_languages()
                        # Get README content if available
                        try:
                            readme = repo.get_readme()
                            readme_content = readme.decoded_content.decode('utf-8')
                            # Truncate very long READMEs
                            if len(readme_content) > 5000:
                                readme_content = readme_content[:5000] + "..."
                        except:
                            readme_content = ""
                        
                        repo_data = {
                            "name": repo.name,
                            "full_name": repo.full_name,
                            "description": repo.description,
                            "url": repo.html_url,
                            "stars": repo.stargazers_count,
                            "forks": repo.forks_count,
                            "created_at": repo.created_at.strftime("%Y-%m-%d"),
                            "updated_at": repo.updated_at.strftime("%Y-%m-%d"),
                            "topics": repo.get_topics(),
                            "primary_language": repo.language,
                            "languages": languages,
                            "readme_content": readme_content,
                            "owner_type": repo.owner.type,
                            "is_archived": repo.archived
                        }
                        
                        all_repos.append(repo_data)
                        count += 1
                        print(f"  Scraped {count}/{self.limit}: {repo.full_name}")
                        
                    except RateLimitExceededException:
                        self._handle_rate_limit()
                    except Exception as e:
                        print(f"  Error scraping {repo.full_name}: {str(e)}")
                        
            except RateLimitExceededException:
                self._handle_rate_limit()
            except Exception as e:
                print(f"Error with topic {topic}: {str(e)}")
        
        # Save the scraped data
        with open(self.raw_data_path, 'w', encoding='utf-8') as f:
            json.dump(all_repos, f, ensure_ascii=False, indent=2)
            
        print(f"Scraped {len(all_repos)} repositories successfully!")
        return all_repos

if __name__ == "__main__":
    # Test the scraper
    scraper = GitHubScraper(limit=10)
    scraper.scrape_repositories()