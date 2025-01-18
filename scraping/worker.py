# worker.py

# Todo:
# Replace run_worker with proper config according to ListQueueBroker implementation
# Setup worker properly for distributed scraping
# 

import asyncio
from reddit.redis_config import broker
from reddit.parallel_reddit_scraper import scrape_subreddit  # Your task function
import logging
import signal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def health_check():
    """Check if the worker is healthy"""
    try:
        redis_client = await broker.get_redis_client()
        await redis_client.ping()
        return True
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return False

async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {signal.name}...")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    for task in tasks:
        task.cancel()
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    logger.info("Shutdown complete.")


async def main():

    logger.info("Starting Redis worker...")

    loop = asyncio.get_running_loop()
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop))
            )
    
    # Start the worker
    
    
    try:
        await broker.startup()
        logger.info("Redis broker started successfully")
        # Run the worker indefinitely
        await broker.run_worker(
            prefetch_count=10,  # Number of tasks to prefetch
            max_concurrency=5,  # Maximum concurrent tasks
            max_polling_interval=5.0,
            graceful_shutdown_timeout=10.0,
        )
    except Exception as e:
        logger.error(f"Worker encountered an error: {str(e)}")
        raise
    finally:
        logger.info("Shutting down Redis worker...")
        await broker.shutdown()
        logger.info("Redis worker shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected. Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        raise
