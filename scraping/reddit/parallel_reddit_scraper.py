from scraping.reddit.redis_config import get_queue_length
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend
from taskiq import TaskiqResultTimeoutError, TaskiqEvents, TaskiqScheduler
from aioredis import Redis
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
import time
import random
from dotenv import load_dotenv


load_dotenv()



# Potential improvements:
# Circulate between reddit accounts for scraping to mitigate rate limiting

BASE_REDIS = os.getenv("BASE_REDIS", "redis://localhost:6379")

redis_async_result = RedisAsyncResultBackend(redis_url=f"{BASE_REDIS}/0")

broker = ListQueueBroker(
    url=f"{BASE_REDIS}/0",
    queue_name="reddit_scraper",
    result_backend=redis_async_result
    
)


@dataclass
class ScrapingTask:
    subreddit: str
    entity_limit: int
    date_range: DateRange
    fetch_submissions: bool = True



@broker.task
async def scrape_subreddit(task: ScrapingTask) -> List[DataEntity]:
    """Scrape a single subreddit for either submissions or comments."""
    bt.logging.info(f"Scraping subreddit {task.subreddit}")
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
                bt.logging.info(f"Fetching submissions for {task.subreddit}")
                contents = await RedditCustomScraper._fetch_submissions(reddit, subreddit, config)
            else:
                bt.logging.info(f"Fetching comments for {task.subreddit}")
                contents = await RedditCustomScraper._fetch_comments(reddit, subreddit, config)
                
            return [RedditContent.to_data_entity(content) for content in contents if content]
            
    except Exception as e:
        bt.logging.error(f"Failed to scrape subreddit {task.subreddit}: {str(e)}")
        return []

class ParallelRedditScraper:
    """Manages parallel scraping of multiple subreddits."""
    
    def __init__(self, max_concurrent: int = 5):
        self.max_concurrent = max_concurrent


    async def _scrape_one(self, subreddit: str, entity_limit: int, date_range: DateRange, fetch_submissions: bool, timeout: int) -> List[DataEntity]:

        bt.logging.info(f"Scraping of {subreddit} started")
        start_time = time.time()

        

        task = await scrape_subreddit.kiq(ScrapingTask(subreddit=subreddit, entity_limit=entity_limit, date_range=date_range, fetch_submissions=fetch_submissions))
        bt.logging.info(f"Task created for {subreddit}")

        queue_length = await get_queue_length(broker)
        bt.logging.info(f"Queue length: {queue_length}")

        task_result = await task.wait_result(timeout=timeout)
        bt.logging.info(f"Task result for {subreddit}: {task_result}")

        bt.logging.info(f"Scraping of {subreddit} finished in {time.time() - start_time} seconds")
        return task_result.return_value

    async def scrape_one(self, subreddit: str, entity_limit: int, date_range: DateRange, fetch_submissions: bool, timeout: int) -> List[DataEntity]:
        bt.logging.info(f"Starting scrape of {subreddit}")

        try:
            return await asyncio.wait_for(self._scrape_one(subreddit, entity_limit, date_range, fetch_submissions, timeout-1), timeout)
        
        except (asyncio.TimeoutError, TaskiqResultTimeoutError):
            bt.logging.error(f"Timeout ({timeout-1}) exceeded for {subreddit}")
        
        except Exception as e:
            bt.logging.error(f"Error during scraping of {subreddit}: {str(e)}")
        
        return []
        
    async def scrape_multiple(
        self,
        subreddits: List[str],
        entity_limit: int,
        date_range: DateRange,
    ) -> List[DataEntity]:
        """
        Scrape multiple subreddits in parallel using Redis tasks.
        """
        fetch_submissions = bool(random.getrandbits(1))
        try:
            bt.logging.info(f"Scraping {subreddits} with config: {entity_limit}, {date_range}")
            #A list of lists of Subreddit scraping results
            scraping_results = [
                result for result in await asyncio.gather(*[
                    self.scrape_one(subreddit,entity_limit,date_range,fetch_submissions,1200)
                    for subreddit in subreddits
                ])
            ]
            
            return scraping_results
        except Exception as e:
            bt.logging.error(f"batch scraping failed: {str(e)}")
            return []

    
    
    def _batch_tasks(self, tasks, batch_size):
        """Split tasks into batches for rate limiting."""
        return [tasks[i:i + batch_size] for i in range(0, len(tasks), batch_size)]