from abc import ABC, abstractmethod

from pydantic import BaseModel, model_validator
from pydantic.dataclasses import dataclass


class ClientRequest(BaseModel):
    raw_data: bytes

    api_key: int
    api_version: int
    correlation_id: int
    describe_topic_partition: DescribeTopicPartitionRequest | None = None

    @model_validator(mode="before")
    @classmethod
    def parse_raw_data(cls, raw_data):
        if isinstance(raw_data, bytes):
            api_key = int.from_bytes(raw_data[4:6], "big")
            api_version = int.from_bytes(raw_data[6:8], "big")
            correlation_id = int.from_bytes(raw_data[8:12], "big")
            client_id_length = int.from_bytes(raw_data[12:14], "big")
            describe_topic_partition = None
            if api_key == 75:
                describe_topic_partition = DescribeTopicPartitionRequest.model_validate(
                    raw_data[14 + client_id_length + 1 :]
                )

            return {
                "raw_data": raw_data,
                "api_version": api_version,
                "correlation_id": correlation_id,
                "describe_topic_partition": describe_topic_partition,
                "api_key": api_key,
            }

        return raw_data


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
    def parse_raw_data(cls, raw_data):
        if isinstance(raw_data, bytes):
            array_length = int.from_bytes(raw_data[0:1], "big", signed=True)
            topics = []
            for _ in range(array_length - 1):
                topic_name_length = (
                    int.from_bytes(raw_data[1:2], "big", signed=True) - 1
                )
                topic_name = raw_data[2 : 2 + topic_name_length].decode("utf-8")
                topics.append(DescribeTopicRequest(topic_name=topic_name))
                raw_data = raw_data[2 + topic_name_length + 1 :]

            return {
                "raw_data": raw_data,
                "topics": topics,
                "cursor": int.from_bytes(raw_data[4:5], "big", signed=True),
                "tag_buffer": int.from_bytes(raw_data[5:6], "big", signed=True),
            }

        return raw_data


class TopicResponse(BaseModel):
    error_code: int
    topic_name: str
    topic_id: str
    is_internal: bool
    partitions: int
    topic_authorized_operations: int
    tag_buffer: int

    def serialize(self) -> bytes:
        return (
            self.error_code.to_bytes(2, "big")
            + self.topic_name.encode("utf-8")
            + self.topic_id.encode("utf-8")
            + (1 if self.is_internal else 0).to_bytes(1, "big")
            + self.partitions.to_bytes(1, "big")
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
        topics_array_bytes = (len(self.topics) + 1).to_bytes(1, "big")
        total_topics_bytes = b"".join([topic.serialize() for topic in self.topics])
        next_cursor_bytes = self.next_cursor.to_bytes(1, "big", signed=True)
        tag_buffer_bytes = self.tag_buffer.to_bytes(1, "big", signed=True)

        return (
            throttle_time_ms_bytes
            + topics_array_bytes
            + total_topics_bytes
            + next_cursor_bytes
            + tag_buffer_bytes
        )

    def size(self) -> int:
        return len(self.serialize())


class ServerResponse(BaseModel):
    header: HeaderResponse
    body: BodyResponse

    def serialize(self) -> bytes:
        message_size = (self.body.size() + self.header.size()).to_bytes(4, "big")
        return message_size + self.header.serialize() + self.body.serialize()
