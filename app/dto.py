import io
import traceback
import uuid
from abc import ABC, abstractmethod

from pydantic import BaseModel, model_validator
from pydantic.dataclasses import dataclass

from app.constants import ApiKey
from app.metadata import ClusterMetadataPartition
from app.models import CompactArray, CompactString


class ClientRequest(BaseModel):
    raw_data: bytes

    api_key: ApiKey
    api_version: int
    correlation_id: int
    describe_topic_partition: DescribeTopicPartitionRequest | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_raw_data(cls, raw_data):
        try:
            if isinstance(raw_data, bytes):
                stream = io.BytesIO(raw_data)
                _message_size = int.from_bytes(stream.read(4), "big")
                api_key = int.from_bytes(stream.read(2), "big")
                api_version = int.from_bytes(stream.read(2), "big")
                correlation_id = int.from_bytes(stream.read(4), "big")
                client_id_length = int.from_bytes(stream.read(2), "big", signed=True)

                if client_id_length >= 0:
                    _client_id = stream.read(client_id_length)

                if api_key == ApiKey.DESCRIBE_TOPIC_PARTITIONS or (
                    api_key == ApiKey.API_VERSIONS and api_version >= 3
                ):
                    _tag_buffer = stream.read(1)

                describe_topic_partition = None
                if api_key == ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                    describe_topic_partition = (
                        DescribeTopicPartitionRequest.model_validate(
                            stream.getvalue()[stream.tell() :]
                        )
                    )

                return {
                    "raw_data": raw_data,
                    "api_key": api_key,
                    "api_version": api_version,
                    "correlation_id": correlation_id,
                    "describe_topic_partition": describe_topic_partition,
                }

            return raw_data
        except Exception:
            traceback.print_exc()
            return None


class HeaderResponse(BaseModel):
    correlation_id: int
    api_version: int

    def serialize(self) -> bytes:
        correlation_id = self.correlation_id.to_bytes(4, "big")
        if self.api_version == 0:
            tag_buffer = (0).to_bytes(1, "big")
            return correlation_id + tag_buffer
        else:
            return correlation_id

    def size(self) -> int:
        return len(self.serialize())


class BodyResponse(BaseModel, ABC):
    @abstractmethod
    def serialize(self) -> bytes:
        pass

    @abstractmethod
    def size(self) -> int:
        pass


@dataclass
class ApiVersion:
    api_key: int
    min_version: int
    max_version: int
    tag_buffer: int = 0

    def serialize(self) -> bytes:
        return (
            self.api_key.to_bytes(2, "big")
            + self.min_version.to_bytes(2, "big")
            + self.max_version.to_bytes(2, "big")
            + self.tag_buffer.to_bytes(1, "big")
        )


class ApiVersionsResponse(BodyResponse):
    error_code: int
    api_keys: list[ApiVersion]
    throttle_time_ms: int

    def serialize(self) -> bytes:
        api_keys_length = (len(self.api_keys) + 1).to_bytes(1, "big")
        api_keys_bytes = b"".join([api_key.serialize() for api_key in self.api_keys])
        return (
            self.error_code.to_bytes(2, "big")
            + api_keys_length
            + api_keys_bytes
            + self.throttle_time_ms.to_bytes(4, "big")
            + b"\x00"
        )

    def size(self) -> int:
        return len(self.serialize())


class DescribeTopicRequest(BaseModel):
    topic_name: str
    tag_buffer: int = 0


class DescribeTopicPartitionRequest(BaseModel):
    raw_data: bytes

    topics: list[DescribeTopicRequest]
    cursor: int = -1
    tag_buffer: int = 0

    @model_validator(mode="before")
    @classmethod
    def parse_raw_data(cls, raw_data: bytes):
        if isinstance(raw_data, bytes):
            stream = io.BytesIO(raw_data)
            items = CompactArray.deserialize_strings(stream)
            topics = []

            for item in items:
                topics.append(DescribeTopicRequest(topic_name=item))

            return {
                "raw_data": raw_data,
                "topics": topics,
                "cursor": int.from_bytes(stream.read(1), "big", signed=True),
                "tag_buffer": int.from_bytes(stream.read(1), "big", signed=True),
            }

        return raw_data


class TopicResponse(BaseModel):
    error_code: int
    topic_name: str
    topic_id: uuid.UUID
    is_internal: bool
    partitions: ClusterMetadataPartition
    topic_authorized_operations: int
    tag_buffer: int

    def serialize(self) -> bytes:
        compact_string = CompactString.serialize(self.topic_name)
        return (
            self.error_code.to_bytes(2, "big")
            + compact_string
            + self.topic_id.bytes
            + (1 if self.is_internal else 0).to_bytes(1, "big")
            + (1).to_bytes(1, "big")  # TODO: change this
            + self.topic_authorized_operations.to_bytes(4, "big")
            + self.tag_buffer.to_bytes(1, "big")
        )


class DescribeTopicBodyResponse(BodyResponse):
    throttle_time_ms: int
    topics: list[TopicResponse]
    next_cursor: int = -1
    tag_buffer: int = 0

    def serialize(self) -> bytes:
        throttle_time_ms_bytes = self.throttle_time_ms.to_bytes(4, "big")
        topic_bytes = CompactArray.serialize_compact_array(
            self.topics, lambda topic: topic.serialize()
        )
        next_cursor_bytes = self.next_cursor.to_bytes(1, "big", signed=True)
        tag_buffer_bytes = self.tag_buffer.to_bytes(1, "big", signed=True)

        return (
            throttle_time_ms_bytes + topic_bytes + next_cursor_bytes + tag_buffer_bytes
        )

    def size(self) -> int:
        return len(self.serialize())


class ServerResponse(BaseModel):
    header: HeaderResponse
    body: BodyResponse

    def serialize(self) -> bytes:
        message_size = (self.body.size() + self.header.size()).to_bytes(4, "big")
        result = message_size + self.header.serialize() + self.body.serialize()
        print("result", result)
        return result
