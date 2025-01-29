# redis_config.py
import os
from common.data import DataEntity
from enum import Enum
from taskiq_redis import ListQueueBroker, RedisAsyncResultBackend
from taskiq import TaskiqEvents, TaskiqScheduler
from aioredis import Redis
import bittensor as bt
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Union
from dataclasses import asdict
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




async def get_queue_length(broker) -> int:
    """
    Get the current length of the reddit_scraper queue from Redis.
    
    Args:
        broker: The broker instance containing the Redis connection pool
        
    Returns:
        int: The current length of the queue
    """
    async with Redis(connection_pool=broker.connection_pool) as redis_conn:
        queue_length = await redis_conn.llen("reddit_scraper")
        bt.logging.info(f"Current queue length: {queue_length}")
        return queue_length





class DataEntitySerializer:
    @staticmethod
    def to_json(entity: DataEntity) -> str:
        """
        Serialize a DataEntity object to JSON string.
        Handles Pydantic model serialization with proper type conversion.
        """
        # Convert to dict using Pydantic's built-in method
        entity_dict = entity.model_dump()
        
        # Handle special field conversions
        entity_dict['datetime'] = entity_dict['datetime'].isoformat()
        entity_dict['content'] = entity_dict['content'].decode('utf-8') if isinstance(entity_dict['content'], bytes) else entity_dict['content']
        
        # Handle Enum serialization for DataSource
        if isinstance(entity_dict['source'], Enum):
            entity_dict['source'] = entity_dict['source'].value
            
        return json.dumps(entity_dict)

    @staticmethod
    def from_json(json_str: str) -> DataEntity:
        """
        Deserialize a JSON string back to a DataEntity object.
        Ensures proper type conversion for Pydantic model.
        """
        data = json.loads(json_str)
        
        # Convert ISO format string back to datetime
        if isinstance(data['datetime'], str):
            data['datetime'] = datetime.fromisoformat(data['datetime'])
        
        # Convert content string back to bytes
        if isinstance(data['content'], str):
            data['content'] = data['content'].encode('utf-8')
            
        # DataSource should be handled automatically by Pydantic's type system
        # as long as it's a valid value for the enum
            
        # Create new DataEntity using Pydantic's type validation
        return DataEntity(**data)

    @staticmethod
    def serialize_list(entities: List[DataEntity]) -> str:
        """
        Serialize a list of DataEntity objects.
        """
        return json.dumps([json.loads(DataEntitySerializer.to_json(entity)) for entity in entities])

    @staticmethod
    def deserialize_list(json_str: str) -> List[DataEntity]:
        """
        Deserialize a JSON string back to a list of DataEntity objects.
        """
        data_list = json.loads(json_str)
        return [DataEntitySerializer.from_json(json.dumps(item)) for item in data_list]


        
# Create broker instance

# Optional: Configure broker events
@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def on_worker_startup(task_name: str, **kwargs):
    print(f"Starting task: {task_name}")

@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def on_worker_shutdown(task_name: str, **kwargs):
    print(f"Completed task: {task_name}")

