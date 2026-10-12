from enum import IntEnum


class ApiKey(IntEnum):
    FETCH = 1
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75


class ErrorCode(IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35