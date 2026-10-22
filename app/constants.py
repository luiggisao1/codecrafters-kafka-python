from enum import IntEnum


class ApiKey(IntEnum):
    FETCH = 1
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75


class ErrorCode(IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35


class TopicRecordType(IntEnum):
    FEATURE_LEVEL_RECORD = 12
    TOPIC_RECORD = 2
    PARTITION_RECORD = 3


METADATA_PATH = "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
