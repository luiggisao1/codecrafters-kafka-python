#!/usr/bin/env python3
"""
generate_cluster_metadata.py

Utility to generate a Kafka KRaft `__cluster_metadata-0/00000000000000000000.log` file
for local development and testing.
"""

import argparse
import struct
import uuid
from pathlib import Path


def _make_crc32c_table() -> list[int]:
    poly = 0x82F63B78
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ poly if (crc & 1) else (crc >> 1)
        table.append(crc)
    return table


_CRC32C_TABLE = _make_crc32c_table()


def crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for b in data:
        crc = _CRC32C_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
    return (crc ^ 0xFFFFFFFF) & 0xFFFFFFFF


def encode_uvarint(val: int) -> bytes:
    res = bytearray()
    while (val & ~0x7F) != 0:
        res.append((val & 0x7F) | 0x80)
        val >>= 7
    res.append(val & 0x7F)
    return bytes(res)


def encode_varint(val: int) -> bytes:
    zigzag = ((val << 1) ^ (val >> 31)) & 0xFFFFFFFF
    return encode_uvarint(zigzag)


def encode_compact_string(s: str) -> bytes:
    data = s.encode("utf-8")
    return encode_uvarint(len(data) + 1) + data


def encode_compact_int32_array(arr: list[int]) -> bytes:
    res = bytearray(encode_uvarint(len(arr) + 1))
    for item in arr:
        res.extend(struct.pack(">i", item))
    return bytes(res)


def build_feature_level_record_value(name: str = "metadata.version", level: int = 14) -> bytes:
    frame_version = encode_uvarint(1)
    api_key = encode_uvarint(12)  # FeatureLevelRecord
    version = encode_uvarint(0)
    name_bytes = encode_compact_string(name)
    level_bytes = struct.pack(">h", level)
    tagged_fields = encode_uvarint(0)
    return frame_version + api_key + version + name_bytes + level_bytes + tagged_fields


def build_topic_record_value(topic_name: str, topic_uuid: uuid.UUID) -> bytes:
    frame_version = encode_uvarint(1)
    api_key = encode_uvarint(2)  # TopicRecord
    version = encode_uvarint(0)
    name_bytes = encode_compact_string(topic_name)
    uuid_bytes = topic_uuid.bytes
    tagged_fields = encode_uvarint(0)
    return frame_version + api_key + version + name_bytes + uuid_bytes + tagged_fields


def build_partition_record_value(
    partition_id: int,
    topic_uuid: uuid.UUID,
    leader: int = 1,
    replicas: list[int] | None = None,
) -> bytes:
    if replicas is None:
        replicas = [1]
    frame_version = encode_uvarint(1)
    api_key = encode_uvarint(3)  # PartitionRecord
    version = encode_uvarint(0)
    part_id_bytes = struct.pack(">i", partition_id)
    uuid_bytes = topic_uuid.bytes
    replicas_bytes = encode_compact_int32_array(replicas)
    isr_bytes = encode_compact_int32_array(replicas)
    removing_bytes = encode_compact_int32_array([])
    adding_bytes = encode_compact_int32_array([])
    leader_bytes = struct.pack(">i", leader)
    leader_epoch_bytes = struct.pack(">i", 0)
    part_epoch_bytes = struct.pack(">i", 0)
    tagged_fields = encode_uvarint(0)
    return (
        frame_version
        + api_key
        + version
        + part_id_bytes
        + uuid_bytes
        + replicas_bytes
        + isr_bytes
        + removing_bytes
        + adding_bytes
        + leader_bytes
        + leader_epoch_bytes
        + part_epoch_bytes
        + tagged_fields
    )


def build_record(offset_delta: int, value: bytes) -> bytes:
    body = bytearray()
    body.append(0)  # attributes
    body.extend(encode_varint(0))  # timestampDelta: 0
    body.extend(encode_varint(offset_delta))  # offsetDelta
    body.extend(encode_varint(-1))  # keyLength: -1 (null key)
    body.extend(encode_varint(len(value)))  # valueLen
    body.extend(value)  # value bytes
    body.extend(encode_varint(0))  # headersCount: 0
    return encode_varint(len(body)) + bytes(body)


def build_record_batch(records: list[bytes], base_offset: int = 0) -> bytes:
    records_count = len(records)
    records_data = b"".join(records)
    tail_header = struct.pack(
        ">h i q q q h i i",
        0,  # attributes
        records_count - 1,  # last_offset_delta
        1700000000000,  # baseTimestamp
        1700000000000,  # maxTimestamp
        -1,  # producerId
        -1,  # producerEpoch
        -1,  # baseSequence
        records_count,  # recordsCount
    )
    crc_payload = tail_header + records_data
    crc_val = crc32c(crc_payload)
    front_header = struct.pack(">i b I", 0, 2, crc_val)  # leader_epoch, magic (2), crc
    batch_content = front_header + crc_payload
    return struct.pack(">q i", base_offset, len(batch_content)) + batch_content


def generate_cluster_metadata_log(
    topics: list[tuple[str, uuid.UUID]],
    include_feature_level: bool = True,
    include_partition_record: bool = True,
) -> bytes:
    records = []
    delta = 0

    if include_feature_level:
        records.append(build_record(delta, build_feature_level_record_value()))
        delta += 1

    for topic_name, topic_uuid in topics:
        records.append(build_record(delta, build_topic_record_value(topic_name, topic_uuid)))
        delta += 1
        if include_partition_record:
            records.append(build_record(delta, build_partition_record_value(0, topic_uuid)))
            delta += 1

    return build_record_batch(records, base_offset=0)


def main():
    parser = argparse.ArgumentParser(description="Generate Kafka __cluster_metadata log file")
    parser.add_argument("--topic", default="foo", help="Topic name (default: foo)")
    parser.add_argument(
        "--uuid",
        default="f81d4fae-7dec-11d0-a765-00a0c91e6bf6",
        help="Topic UUID (default: f81d4fae-7dec-11d0-a765-00a0c91e6bf6)",
    )
    parser.add_argument(
        "--out",
        nargs="+",
        default=[
            "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log",
            "test_data/__cluster_metadata-0/00000000000000000000.log",
        ],
        help="Output file paths",
    )
    args = parser.parse_args()

    topic_uuid = uuid.UUID(args.uuid)
    log_bytes = generate_cluster_metadata_log(
        topics=[(args.topic, topic_uuid)],
        include_feature_level=True,
        include_partition_record=True,
    )

    for out_path_str in args.out:
        p = Path(out_path_str)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(log_bytes)
        print(f"Generated {len(log_bytes)} bytes at: {p.resolve()}")

    print()
    print("Log Contents:")
    print(f"  Topic: {args.topic}")
    print(f"  UUID:  {topic_uuid}")
    print(f"  UUID Hex: {topic_uuid.bytes.hex()}")


if __name__ == "__main__":
    main()
