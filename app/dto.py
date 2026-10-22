import io
import traceback
import uuid
from abc import ABC, abstractmethod
from typing import Any

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


class DescribeTopicPartitionResponse(BaseModel):
    error_code: int
    partition_index: int
    leader_id: int
    leader_epoch: int
    replica_nodes: list[int]
    isr_nodes: list[int]
    eligible_replica_nodes: list[int]
    last_known_elr: list[int]
    offline_replicas: list[int]
    tag_buffer: int

    @classmethod
    def from_cluster_metadata(cls, data: ClusterMetadataPartition, partition_idx: int):
        return cls(
            error_code=0,
            partition_index=partition_idx,
            leader_id=data.leader_id,
            leader_epoch=data.leader_epoch,
            replica_nodes=data.replicas,
            isr_nodes=data.isr_replicas,
            eligible_replica_nodes=[],
            last_known_elr=[],  # TODO: check these values
            offline_replicas=[],
            tag_buffer=0,
        )

    def serialize(self):
        return (
            self.error_code.to_bytes(2, "big")
            + self.partition_index.to_bytes(4, "big")
            + self.leader_id.to_bytes(4, "big")
            + self.leader_epoch.to_bytes(4, "big")
            + CompactArray.serialize_compact_array(
                self.replica_nodes, lambda node: node.to_bytes(4, "big")
            )
            + CompactArray.serialize_compact_array(
                self.isr_nodes, lambda node: node.to_bytes(4, "big")
            )
            + CompactArray.serialize_compact_array(
                self.eligible_replica_nodes, lambda node: node.to_bytes(4, "big")
            )
            + CompactArray.serialize_compact_array(
                self.last_known_elr, lambda node: node.to_bytes(4, "big")
            )
            + CompactArray.serialize_compact_array(
                self.offline_replicas, lambda node: node.to_bytes(4, "big")
            )
            + self.tag_buffer.to_bytes(1, "big")
        )


class TopicResponse(BaseModel):
    error_code: int
    topic_name: str
    topic_id: uuid.UUID
    is_internal: bool
    partitions: list[ClusterMetadataPartition]
    topic_authorized_operations: int
    tag_buffer: int

    def serialize(self) -> bytes:
        compact_string = CompactString.serialize(self.topic_name)
        partitions_bytes = []
        for idx, partition in enumerate(self.partitions):
            partitions_bytes.append(
                DescribeTopicPartitionResponse.from_cluster_metadata(
                    partition, idx
                ).serialize()
            )

        partitions_to_bytes = CompactArray.serialize_compact_array(
            partitions_bytes, lambda partition_l: partition_l
        )

        return (
            self.error_code.to_bytes(2, "big")
            + compact_string
            + self.topic_id.bytes
            + (1 if self.is_internal else 0).to_bytes(1, "big")
            + partitions_to_bytes  # TODO: change this
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
