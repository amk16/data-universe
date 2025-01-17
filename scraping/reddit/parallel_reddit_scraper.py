from taskiq import TaskiqEvents, TaskiqScheduler, AsyncBroker
from scraping.reddit.redis_config import broker
from typing import List, Optional
import asyncio
from dataclasses import dataclass
from scraping.reddit.reddit_custom_scraper import RedditCustomScraper  # adjust import path
from scraping.scraper import ScrapeConfig
import asyncpraw
from common.date_range import DateRange
from common.data import DataLabel, DataEntity
import os
import random
import bittensor as bt




@dataclass
class ScrapingTask:
    subreddit: str
    entity_limit: int
    date_range: DateRange
    fetch_submissions: bool = True

@broker.task
async def scrape_subreddit(task: ScrapingTask) -> List[DataEntity]:
    """Scrape a single subreddit for either submissions or comments."""
    try:
        async with asyncpraw.Reddit(
            client_id=os.getenv("REDDIT_CLIENT_ID"),
            client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
            username=os.getenv("REDDIT_USERNAME"),
            password=os.getenv("REDDIT_PASSWORD"),
            user_agent=RedditCustomScraper.USER_AGENT,
        ) as reddit:
            subreddit = await reddit.subreddit(task.subreddit)
            
            config = ScrapeConfig(
                entity_limit=task.entity_limit,
                date_range=task.date_range,
                labels=[DataLabel(value=f"r/{task.subreddit}")]
            )
            
            if task.fetch_submissions:
                contents = await RedditCustomScraper._fetch_submissions(reddit, subreddit, config)
            else:
                contents = await RedditCustomScraper._fetch_comments(reddit, subreddit, config)
                
            return [RedditContent.to_data_entity(content) for content in contents if content]
            
    except Exception as e:
        bt.logging.error(f"Failed to scrape subreddit {task.subreddit}: {str(e)}")
        return []

class ParallelRedditScraper:
    """Manages parallel scraping of multiple subreddits."""
    
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent
        
    async def scrape_multiple(
        self,
        subreddits: List[str],
        entity_limit: int,
        date_range: DateRange,
    ) -> List[DataEntity]:
        """
        Scrape multiple subreddits in parallel using Redis tasks.
        """
        try:
            tasks = []
            for subreddit in subreddits:
                fetch_submissions = bool(random.getrandbits(1))
            
                task = ScrapingTask(
                    subreddit=subreddit,
                    entity_limit=entity_limit,
                    date_range=date_range,
                    fetch_submissions=fetch_submissions
                )
                tasks.append(scrape_subreddit.kiq(task))
            
            results = []
            for batch in self._batch_tasks(tasks, self.max_concurrent):
                batch_results = await asyncio.gather(*batch, return_exceptions=True)

            for result in batch_results:
                if isinstance(result, Exception):
                    bt.logging.error(f"Error during scraping: {result}")
                    continue
                
                results.extend(result)
            
            return results
        except Exception as e:
            bt.logging.error(f"batch scraping failed: {str(e)}")
            return []

    
    
    def _batch_tasks(self, tasks, batch_size):
        """Split tasks into batches for rate limiting."""
        return [tasks[i:i + batch_size] for i in range(0, len(tasks), batch_size)]