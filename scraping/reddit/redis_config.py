# redis_config.py
import os
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend
from taskiq import TaskiqEvents, TaskiqScheduler


# Redis connection configuration
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "db": int(os.getenv("REDIS_DB", 0)),
    "socket_timeout": 5,  # Add timeout settings
    "socket_connect_timeout": 5
}

BASE_REDIS = os.getenv("BASE_REDIS", "redis://localhost:6379")

redis_async_result = RedisAsyncResultBackend(redis_url=f"{BASE_REDIS}/0")

broker = ListQueueBroker(
    url=f"{BASE_REDIS}/0",
    queue_name="reddit_scraper",
    result_backend=redis_async_result
    
)


# Create Redis URL from config
# def get_redis_url():
#     auth = ""
#     if REDIS_CONFIG["username"] and REDIS_CONFIG["password"]:
#         auth = f"{REDIS_CONFIG['username']}:{REDIS_CONFIG['password']}@"
#     elif REDIS_CONFIG["password"]:
#         auth = f":{REDIS_CONFIG['password']}@"
    
#     return f"redis://{auth}{REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}/{REDIS_CONFIG['db']}"
async def check_redis_connection():
    try:
        redis_client = await broker.get_redis_client()
        await redis_client.ping()
        return True
    except Exception as e:
        print(f"Redis connection failed: {e}")
        return False
        
# Create broker instance

# Optional: Configure broker events
@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def on_worker_startup(task_name: str, **kwargs):
    print(f"Starting task: {task_name}")

@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def on_worker_shutdown(task_name: str, **kwargs):
    print(f"Completed task: {task_name}")

