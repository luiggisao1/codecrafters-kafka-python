import io


def encode_uvarint(value: int) -> bytes:
    buffer = bytearray()
    while value >= 0x80:
        buffer.append((value & 0x7F) | 0x80)
        value >>= 7
    buffer.append(value & 0x7F)
    return bytes(buffer)


def decode_uvarint(stream: io.BytesIO) -> int:
    result = 0
    shift = 0
    while True:
        raw_byte = stream.read(1)
        if not raw_byte:
            raise EOFError("Unexpected EOF while reading uvarint")
        byte = raw_byte[0]
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return result


def decode_varint(stream):
    # 1. Read the raw variable-length integer using your existing method
    raw = decode_uvarint(stream)

    # 2. Undo the ZigZag encoding (1 line of arithmetic)
    return (raw >> 1) ^ -(raw & 1)


# 0000 0010
# 0000 0100 - shift 1
# 0000 1000 - shift 2
# 0001 0000 - shift 3
# 0010 0000 - shift 4
# 0100 0000 - shift 5
# 1000 0000 - shift 6
# 1 0000 0000 - shift 7
