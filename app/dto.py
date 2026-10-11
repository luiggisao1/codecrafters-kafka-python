from typing import Any

from pydantic.dataclasses import dataclass


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


@dataclass
class ApiVersionsResponse:
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

@dataclass
class ServerResponse:
    correlation_id: int
    body: ApiVersionsResponse

    def serialize(self) -> bytes:
        message_size = (len(self.body.serialize()) + 4).to_bytes(4, "big")
        return (
            message_size
            + self.correlation_id.to_bytes(4, "big")
            + self.body.serialize()
        )