import uuid

from app.constants import ApiKey, ErrorCode
from app.dto import (
    ApiVersion,
    ApiVersionsResponse,
    ClientRequest,
    DescribeTopicBodyResponse,
    HeaderResponse,
    ServerResponse,
    TopicResponse,
)


def handle_api_versions(client_request: ClientRequest) -> ServerResponse:
    header = HeaderResponse(
        correlation_id=client_request.correlation_id,
        api_version=client_request.api_version,
    )
    is_valid_version = 0 <= client_request.api_version <= 4
    error_code = ErrorCode.NONE if is_valid_version else ErrorCode.UNSUPPORTED_VERSION

    body = ApiVersionsResponse(
        error_code=error_code,
        api_keys=[
            ApiVersion(api_key=ApiKey.API_VERSIONS, min_version=0, max_version=4, tag_buffer=0),
            ApiVersion(api_key=ApiKey.DESCRIBE_TOPIC_PARTITIONS, min_version=0, max_version=0, tag_buffer=0),
        ],
        throttle_time_ms=0,
    )
    return ServerResponse(header=header, body=body)


def handle_describe_topic_partitions(client_request: ClientRequest) -> ServerResponse:
    header = HeaderResponse(
        correlation_id=client_request.correlation_id,
        api_version=client_request.api_version,
    )
    topics = []
    if client_request.describe_topic_partition:
        for req_topic in client_request.describe_topic_partition.topics:
            topics.append(
                TopicResponse(
                    error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    topic_name=req_topic.topic_name,
                    partitions=0,
                    topic_id=uuid.UUID("00000000-0000-0000-0000-000000000000"),
                    is_internal=False,
                    topic_authorized_operations=0,
                    tag_buffer=0,
                )
            )

    body = DescribeTopicBodyResponse(
        throttle_time_ms=0,
        topics=topics,
        next_cursor=-1,
        tag_buffer=0,
    )
    return ServerResponse(header=header, body=body)


HANDLERS = {
    ApiKey.API_VERSIONS: handle_api_versions,
    ApiKey.DESCRIBE_TOPIC_PARTITIONS: handle_describe_topic_partitions,
}


def route_request(client_request: ClientRequest) -> bytes:
    handler = HANDLERS.get(client_request.api_key)
    if handler:
        response = handler(client_request)
    else:
        # Fallback to ApiVersions handler
        response = handle_api_versions(client_request)
    return response.serialize()
