import dataclasses
from taskiq.serializers.json_serializer import JsonSerializer
import time
from typing import Any
import json
from common import constants
from common.date_range import DateRange
from . import utils
import datetime as dt
from enum import IntEnum
from typing import Any, Dict, List, Type, Optional
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PositiveInt,
    field_validator,  # Changed from validator
)


class StrictBaseModel(BaseModel):
    """A BaseModel that enforces stricter validation constraints"""

    model_config = ConfigDict(
        use_enum_values=True
    )


class TimeBucket(StrictBaseModel):
    """Represents a specific time bucket in the linear flow of time."""

    model_config = ConfigDict(frozen=True)

    id: PositiveInt = Field(
        description="Monotonically increasing value idenitifying the given time bucket"
    )

    def __hash__(self) -> int:
        return hash(int(self.id))

    @classmethod
    def from_datetime(cls, datetime: dt.datetime) -> Type["TimeBucket"]:
        """Creates a TimeBucket from the provided datetime.

        Args:
            datetime (datetime.datetime): A datetime object, assumed to be in UTC.
        """
        datetime.astimezone(dt.timezone.utc)
        return TimeBucket(
            id=utils.seconds_to_hours(
                datetime.astimezone(tz=dt.timezone.utc).timestamp()
            )
        )

    @classmethod
    def to_date_range(cls, bucket: "TimeBucket") -> DateRange:
        """Returns the date range for this time bucket."""
        return DateRange(
            start=utils.datetime_from_hours_since_epoch(bucket.id),
            end=utils.datetime_from_hours_since_epoch(bucket.id + 1),
        )


class DataSource(IntEnum):
    """The source of data. This will be expanded over time as we increase the types of data we collect."""

    REDDIT = 1
    X = 2
    UNKNOWN_3 = 3
    UNKNOWN_4 = 4
    UNKNOWN_5 = 5
    UNKNOWN_6 = 6
    UNKNOWN_7 = 7

    @property
    def weight(self):
        weights = {
            DataSource.REDDIT: 0.6,
            DataSource.X: 0.4,
            DataSource.UNKNOWN_3: 0,
            DataSource.UNKNOWN_4: 0,
            DataSource.UNKNOWN_5: 0,
            DataSource.UNKNOWN_6: 0,
            DataSource.UNKNOWN_7: 0
        }
        return weights[self]


class DataLabel(StrictBaseModel):
    """An optional label to classify a data entity."""

    model_config = ConfigDict(frozen=True)

    value: str = Field(
        max_length=32,
        description="The label. E.g. a subreddit for Reddit data.",
    )

    @field_validator("value")  # Changed from validator
    @classmethod
    def lower_case_value(cls, value: str) -> str:
        """Converts the value to lower case to consistent casing throughout the system."""
        if len(value.lower()) > 32:
            raise ValueError(
                f"Label: {value} when is over 32 characters when .lower() is applied: {value.lower()}."
            )
        return value.lower()


class DataEntity(StrictBaseModel):
    """A logical unit of data that has been scraped. E.g. a Reddit post"""

    model_config = ConfigDict(frozen=True)

    uri: str
    datetime: dt.datetime
    source: DataSource
    label: Optional[DataLabel] = Field(default=None)
    content: bytes
    content_size_bytes: int = Field(ge=0)

    @classmethod
    def are_non_content_fields_equal(
            cls, this: "DataEntity", other: "DataEntity"
    ) -> bool:
        return (
                this.uri == other.uri
                and this.datetime == other.datetime
                and this.source == other.source
                and this.label == other.label
        )


class DataEntitySerializer(JsonSerializer):
    def dumps(self, message: Any) -> bytes:
        if isinstance(message, DataEntity):
            data = message.model_dump()
            # Convert datetime to ISO format with 'Z'
            data['datetime'] = data['datetime'].strftime('%Y-%m-%dT%H:%M:%SZ')
            # Keep content as string if it's JSON
            if isinstance(data['content'], bytes):
                data['content'] = data['content'].decode('utf-8')
            # Handle label
            if data['label']:
                data['label'] = {'value': data['label'].value}
            return json.dumps(data).encode()
        return super().dumps(message)
    
    def loads(self, message: bytes) -> Any:
        data = super().loads(message)
        if isinstance(data, dict) and 'uri' in data:
            # Convert datetime string back
            data['datetime'] = dt.datetime.strptime(
                data['datetime'], 
                '%Y-%m-%dT%H:%M:%SZ'
            ).replace(tzinfo=dt.timezone.utc)
            
            # Convert content to bytes if it's a string
            if isinstance(data['content'], str):
                data['content'] = data['content'].encode('utf-8')
            
            # Handle label reconstruction
            if data['label']:
                data['label'] = DataLabel(data['label']['value'])
            
            # Create DataEntity instance
            return DataEntity.model_validate(data)
        return data


class HuggingFaceMetadata(StrictBaseModel):
    repo_name: str
    source: DataSource
    updated_at: dt.datetime
    encoding_key: Optional[str] = None

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        json_encoders={
            dt.datetime: lambda v: v.isoformat(),
        }
    )


class DataEntityBucketId(StrictBaseModel):
    """Uniquely identifies a bucket to group DataEntities by time bucket, source, and label."""

    model_config = ConfigDict(frozen=True)

    time_bucket: TimeBucket
    source: DataSource = Field()
    label: Optional[DataLabel] = Field(default=None)

    def __hash__(self) -> int:
        return hash(hash(self.time_bucket) + hash(self.source) + hash(self.label))


class DataEntityBucket(StrictBaseModel):
    """Summarizes a group of data entities stored by a miner."""

    id: DataEntityBucketId = Field(
        description="Identifies the qualities by which this bucket is grouped."
    )
    size_bytes: int = Field(ge=0, le=constants.DATA_ENTITY_BUCKET_SIZE_LIMIT_BYTES)


@dataclasses.dataclass()
class CompressedEntityBucket:
    """A compressed version of the DataEntityBucket to reduce bytes sent on the wire."""

    label: Optional[str] = None
    time_bucket_ids: List[int] = dataclasses.field(default_factory=list)
    sizes_bytes: List[int] = dataclasses.field(default_factory=list)


class CompressedMinerIndex(BaseModel):
    """A compressed version of the MinerIndex to reduce bytes sent on the wire."""

    sources: Dict[int, List[CompressedEntityBucket]]

    @field_validator("sources")  # Changed from validator
    @classmethod
    def validate_index_size(
            cls, sources: Dict[int, List[CompressedEntityBucket]]
    ) -> Dict[int, List[CompressedEntityBucket]]:
        size = sum(
            len(compressed_bucket.time_bucket_ids)
            for compressed_buckets in sources.values()
            for compressed_bucket in compressed_buckets
        )
        if size > constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4:
            raise ValueError(
                f"Compressed index is too large. {size} buckets > {constants.DATA_ENTITY_BUCKET_COUNT_LIMIT_PER_MINER_INDEX_PROTOCOL_4}"
            )
        return sources

    @classmethod
    def bucket_count(cls, index: "CompressedMinerIndex") -> int:
        return sum(
            len(compressed_bucket.time_bucket_ids)
            for compressed_buckets in index.sources.values()
            for compressed_bucket in compressed_buckets
        )

    @classmethod
    def size_bytes(cls, index: "CompressedMinerIndex") -> int:
        return sum(
            size_byte
            for compressed_buckets in index.sources.values()
            for compressed_bucket in compressed_buckets
            for size_byte in compressed_bucket.sizes_bytes
        )