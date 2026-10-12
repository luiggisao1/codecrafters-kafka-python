import io

from app.utils import encode_uvarint, decode_uvarint


class CompactString:
    @staticmethod
    def serialize(value: str | None) -> bytes:
        if value is None:
            return encode_uvarint(0)
        encoded = value.encode("utf-8")
        return encode_uvarint(len(encoded) + 1) + encoded

    @staticmethod
    def deserialize(stream: io.BytesIO) -> str | None:
        length = decode_uvarint(stream)
        if length == 0:
            return None
        actual_len = length - 1
        return stream.read(actual_len).decode("utf-8")


class CompactArray:
    @staticmethod
    def serialize_compact_array(items: list, item_func) -> bytes:
        out = bytearray(encode_uvarint(len(items) + 1))
        for item in items:
            out.extend(item_func(item))
        return bytes(out)

    @staticmethod
    def deserialize_strings(stream: io.BytesIO) -> list[str]:
        count = decode_uvarint(stream)
        if count == 0:
            return []
        actual_count = count - 1
        return [str(CompactString.deserialize(stream)) for _ in range(actual_count)]
