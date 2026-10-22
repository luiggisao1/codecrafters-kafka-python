import io
from pathlib import Path
import uuid

from app.constants import TopicRecordType
from app.models import CompactString
from app.utils import decode_varint


# def read_metadata(path: Path):
#     # TODO: Make it work for multiple batches
#     stream = io.BytesIO(path.read_bytes())
#     base_offset = int.from_bytes(stream.read(8), byteorder="big")
#     batch_length = int.from_bytes(stream.read(4), byteorder="big")
#     partition_leader_epoch = int.from_bytes(stream.read(4), byteorder="big")
#     magic_byte = int.from_bytes(stream.read(1), byteorder="big")
#     crc = int.from_bytes(stream.read(4), byteorder="big")
#     attributes = int.from_bytes(stream.read(2), byteorder="big")
#     last_offset_delta = int.from_bytes(stream.read(4), byteorder="big")
#     base_timestamp = int.from_bytes(stream.read(8), byteorder="big")
#     max_timestamp = int.from_bytes(stream.read(8), byteorder="big")
#     producer_id = int.from_bytes(stream.read(8), byteorder="big")
#     producer_epoch = int.from_bytes(stream.read(2), byteorder="big")
#     base_sequence = int.from_bytes(stream.read(4), byteorder="big")
#     records_length = int.from_bytes(stream.read(4), byteorder="big")
#     # print("records_length:", records_length, stream.getvalue())
#     for _ in range(records_length):
#         record_length = decode_varint(stream)
#         total_record_length = record_length + 1
#
#         record = stream.read(record_length)
#         record_stream = io.BytesIO(record)
#         record_attributes = int.from_bytes(
#             record_stream.read(1), byteorder="big", signed=True
#         )
#         record_timestamp_delta = decode_varint(record_stream)
#         record_offset_delta = decode_varint(record_stream)
#         key_length = decode_varint(record_stream)
#         if key_length != -1:
#             record_stream.read(key_length)
#         value_length = decode_varint(record_stream)
#         record_value = io.BytesIO(record_stream.read(value_length))
#
#         # Read the record headers
#         record_value_frame_version = record_value.read(1)
#         record_value_type = int.from_bytes(record_value.read(1), byteorder="big")
#         if record_value_type == TopicRecordType.TOPIC_RECORD:  # Topic Record
#             record_value_version = record_value.read(1)
#             record_value_name = CompactString.deserialize(record_value)
#             print("name", record_value_name)
#
#             # TODO: move to a topic record handler
#
#             record_vaue_topic_uuid = record_value.read(16)
#             print("uuid", uuid.UUID(bytes=record_vaue_topic_uuid))
#         else:
#             pass
#
#         # record_value_feature_level = record_value.read(2)
#         # record_value_tag_fields = decode_varint(record_value)
#         #
#         # header_length = decode_varint(record_stream)
#
#         print(
#             f"Record length: {record_length}, Total record length: {total_record_length}"
#         )
#
#     # record
#     # print(
#     #     f"base_offset: {base_offset}, batch_length: {batch_length}, partition_leader_epoch: {partition_leader_epoch}, magic_byte: {magic_byte}, crc: {crc}, attributes: {attributes}, last_offset_delta: {last_offset_delta}, base_timestamp: {base_timestamp}, max_timestamp: {max_timestamp}, producer_id: {producer_id}, producer_epoch: {producer_epoch}, base_sequence: {base_sequence}, record_length: {records_length}"
#     # )
#
#     return stream.getvalue()


class ClusterMetadata:
    def __init__(self, topics: dict[str, uuid.UUID] | None = None):
        self.topics: dict[str, uuid.UUID] = topics or {}

    @classmethod
    def from_file(cls, path: Path) -> "ClusterMetadata":

        topics = {}
        # stream = io.BytesIO(path.read_bytes())
        stream = io.BytesIO(
            b"\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00O\x00\x00\x00\x01\x02\xb0iE|\x00\x00\x00\x00\x00\x00\x00\x00\x01\x91\xe0Z\xf8\x18\x00\x00\x01\x91\xe0Z\xf8\x18\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x01:\x00\x00\x00\x01.\x01\x0c\x00\x11metadata.version\x00\x14\x00\x00\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x9d\x00\x00\x00\x01\x02\xa0d\x06\xf4\x00\x00\x00\x00\x00\x01\x00\x00\x01\x91\xe0[-\x15\x00\x00\x01\x91\xe0[-\x15\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00\x00\x00\x02B\x00\x00\x00\x016\x01\x02\x00\x07bananaq\xa5\x9aQ\x89hO\x8b\x93~v\x93\x03\xdb\xaa\xb7\x00\x00\x90\x01\x00\x00\x02\x01\x82\x01\x01\x03\x01\x00\x00\x00\x00q\xa5\x9aQ\x89hO\x8b\x93~v\x93\x03\xdb\xaa\xb7\x02\x00\x00\x00\x01\x02\x00\x00\x00\x01\x01\x01\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02\x10\x00\x00\x00\x00\x00@\x00\x80\x00\x00\x00\x00\x00\x00\x01\x00\x00"
        )

        while stream.tell() < len(stream.getvalue()):
            base_offset = int.from_bytes(stream.read(8), byteorder="big")
            batch_length = int.from_bytes(stream.read(4), byteorder="big")
            partition_leader_epoch = int.from_bytes(stream.read(4), byteorder="big")
            magic_byte = int.from_bytes(stream.read(1), byteorder="big")
            crc = int.from_bytes(stream.read(4), byteorder="big")
            attributes = int.from_bytes(stream.read(2), byteorder="big")
            last_offset_delta = int.from_bytes(stream.read(4), byteorder="big")
            base_timestamp = int.from_bytes(stream.read(8), byteorder="big")
            max_timestamp = int.from_bytes(stream.read(8), byteorder="big")
            producer_id = int.from_bytes(stream.read(8), byteorder="big")
            producer_epoch = int.from_bytes(stream.read(2), byteorder="big")
            base_sequence = int.from_bytes(stream.read(4), byteorder="big")
            records_length = int.from_bytes(stream.read(4), byteorder="big")
            print("records_length", records_length)
            for _ in range(records_length):
                record_length = decode_varint(stream)
                record = stream.read(record_length)
                record_stream = io.BytesIO(record)
                record_attributes = int.from_bytes(
                    record_stream.read(1), byteorder="big", signed=True
                )
                record_timestamp_delta = decode_varint(record_stream)
                record_offset_delta = decode_varint(record_stream)
                key_length = decode_varint(record_stream)
                if key_length != -1:
                    record_stream.read(key_length)
                value_length = decode_varint(record_stream)
                record_value = io.BytesIO(record_stream.read(value_length))
                # Header of value
                record_value_frame_version = record_value.read(1)
                record_value_type = int.from_bytes(
                    record_value.read(1), byteorder="big"
                )
                print("record_value_type", record_value_type)
                if record_value_type == TopicRecordType.TOPIC_RECORD:  # TopicRecord
                    record_value_version = record_value.read(1)
                    print("record_value_version", record_value_version)
                    topic_name = CompactString.deserialize(record_value)
                    topic_uuid = uuid.UUID(bytes=record_value.read(16))

                    if topic_name:
                        topics[topic_name] = topic_uuid
            print("tell", stream.tell(), len(stream.getvalue()))
        return cls(topics=topics)
