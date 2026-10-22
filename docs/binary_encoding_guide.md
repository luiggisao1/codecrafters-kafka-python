# Low-Level Binary Protocols: Varints, ZigZag Encoding & Kafka Internals

An in-depth engineering guide on binary serialization, two's complement, bitwise arithmetic, and how Apache Kafka uses ZigZag-encoded variable-length integers to achieve high-throughput disk and network efficiency.

---

## 1. Why Binary Protocols Matter

Modern web APIs frequently use JSON or Protobuf/gRPC over HTTP. While JSON is human-readable, it carries significant overhead:
* Numbers like `1000000` consume 7 ASCII bytes (`"1000000"`) instead of 4 raw binary bytes.
* Text parsers must scan every character, allocate strings, and convert ASCII to machine numbers on every request.

High-throughput streaming platforms like **Apache Kafka** handle trillions of events per day. To minimize CPU, memory, and network bottlenecks, Kafka communicates and writes to disk using a **compact binary wire protocol**.

---

## 2. Foundations: Bits, Bytes, and Bit Shifting

### Numbers in Binary & Hexadecimal
* **1 byte** = 8 bits ($2^8 = 256$ possible values).
* **1 hex digit (nibble)** = 4 bits ($2^4 = 16$ possible values: `0-9`, `A-F`).
* Every byte is neatly represented by **two hexadecimal digits** (e.g., `0xFF = 255`).

### What Bit Shifting Actually Means
Shifting is the binary equivalent of sliding digits in decimal:
* In **base-10**: Shifting `25` left by 1 digit gives `250` ($\times 10$).
* In **base-2**: Shifting bits left by 1 position **multiplies by 2**; shifting right **divides by 2**.

```text
Number: 3    -->  Binary: 0 0 1 1
Slide Left:       Binary: 0 1 1 0   -->  Equals: 6  (3 × 2)
Slide Left:       Binary: 1 1 0 0   -->  Equals: 12 (6 × 2)
```

Because $16 = 2^4$, **shifting by 4 bits is identical to multiplying or dividing by 16 (shifting by 1 hex digit)**:
```python
0x05 << 4  # Results in 0x50 (5 * 16 = 80)
0x50 >> 4  # Results in 0x05 (80 / 16 = 5)
```

> **Performance Note**: Bit shifts are single-cycle CPU instructions. In hardware, shifting is simply routing wires one pin over—infinitely faster than general arithmetic division.

---

## 3. Signed Integers & The Two's Complement Catastrophe

Computers do not have a physical "minus sign" (`-`) bit. Negative numbers are stored using **Two's Complement**:

* Positive numbers have a leading `0` as their most significant bit (MSB).
* Negative numbers have a leading `1` as their MSB.
* To represent $-1$, the computer counts backwards from 0, causing all bits to roll over to `1`:

| Number | 8-Bit Binary | 32-Bit Hexadecimal |
| :---: | :---: | :---: |
| **`0`** | `0000 0000` | `0x00000000` |
| **`1`** | `0000 0001` | `0x00000001` |
| **`-1`** | `1111 1111` | `0xFFFFFFFF` |
| **`-2`** | `1111 1110` | `0xFFFFFFFE` |

### The Problem with Normal Varints (LEB128)
A **Variable-Length Integer (Varint)** saves space by storing numbers in 7-bit chunks. The 8th bit (MSB) acts as a **continuation flag**:
* `0`: Last byte, stop reading.
* `1`: More bytes follow, keep reading.

For positive numbers like `1`, it takes only **1 byte** (`0x01`).

However, because $-1$ is `0xFFFFFFFF` (all 32 bits are `1`), a standard Varint will keep reading continuation bits and end up writing **5 full bytes** (`0xFF 0xFF 0xFF 0xFF 0x0F`) just to store the number $-1$!

In Kafka, message records with `null` keys use `keyLength = -1`. Wasting 5 to 10 bytes on every single null key would destroy performance.

---

## 4. The Solution: ZigZag Encoding

Invented at Google for Protocol Buffers and adopted by Apache Kafka in Message Format v2 (KIP-98), **ZigZag encoding** folds negative numbers into positive numbers:

```text
Original Number Line:
... -3   -2   -1    0    1    2    3 ...
     ↓    ↓    ↓    ↓    ↓    ↓    ↓
ZigZag Mapped:
...  5    3    1    0    2    4    6 ...
```

* **Positive numbers** become **even numbers** ($2n$).
* **Negative numbers** become **odd numbers** ($2|n| - 1$).

Now both small positive numbers (`+1`, `+2`) and small negative numbers (`-1`, `-2`) stay close to 0 and fit into a single byte!

### The Bitwise Formulas

#### Encoding ($n \to \text{ZigZag}$):
$$\text{zigzag} = (n \ll 1) \ \oplus \ (n \gg 31)$$

#### Decoding ($\text{ZigZag} \to n$):
$$n = (\text{raw} \gg 1) \ \oplus \ -(\text{raw} \ \& \ 1)$$

### Step-by-Step Bit Transition Example:

Watch what happens to **$-1$** in 8-bit arithmetic:
1. **Start with $-1$**: `1111 1111`
2. **Shift Left by 1 (`<< 1`)**: `1111 1110` (a `0` enters on the right).
3. **Arithmetic Shift Right by 7 (`>> 7`)**: `1111 1111` (all `1`s mask).
4. **XOR them together**:
   ```text
       1111 1110
   XOR 1111 1111   <-- XOR with all 1s flips every bit!
   -------------
       0000 0001   <-- Decoded to decimal 1!
   ```

$-1$ is converted to $1$, which safely encodes into **a single byte** (`0x01`) in a Varint stream.

---

## 5. Real-World Case Study: Apache Kafka KRaft Metadata

In Kafka's KRaft (Kafka Raft) mode, cluster state is stored on disk in the metadata partition:
`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`

### Record Batch Layout
On disk, batches are structured as:
1. **`baseOffset`** (`int64`, 8 bytes)
2. **`batchLength`** (`int32`, 4 bytes)
3. **`partitionLeaderEpoch`** (`int32`, 4 bytes)
4. **`magic`** (`int8`, 1 byte) = `2`
5. **`crc`** (`int32`, 4 bytes) = CRC-32C (Castagnoli polynomial `0x82F63B78`) over `attributes` to end of batch.
6. **`attributes`** (`int16`, 2 bytes)
7. **Timestamps, Producer IDs, and Epochs**
8. **`records_count`** (`int32`, 4 bytes)
9. **Record Payload(s)**

### Why ZigZag Stumbled Most Parsers
Inside each record, Kafka defines:
```text
Record:
  length          : varint (Signed ZigZag!)
  attributes      : int8
  timestampDelta  : varint (Signed ZigZag)
  offsetDelta     : varint (Signed ZigZag)
  keyLength       : varint (Signed ZigZag, -1 for null)
  valueLen        : varint (Signed ZigZag)
  value           : byte[] (TopicRecord, PartitionRecord, etc.)
  headersCount    : varint (Signed ZigZag)
```

If a parser reads the `length` field using standard unsigned Varint:
* The first byte on disk is `0x3a` ($58$ in decimal).
* An unsigned reader gets $58$ and advances the cursor by $58$ bytes.
* In reality, ZigZag decoding $58$ gives:
  $$\frac{58}{2} = \mathbf{29 \text{ bytes}}$$
* Advancing by $58$ instead of $29$ causes the parser to jump into the middle of the next record and read corrupted data!

---

## 6. Python Implementation Reference

Here are clean, tested implementations for reading both unsigned and ZigZag-encoded varints from a binary stream:

```python
import io

def decode_uvarint(stream: io.BufferedReader) -> int:
    """
    Reads an unsigned variable-length integer (LEB128).
    Used for: compact string lengths, compact array sizes, tagged fields.
    """
    value = 0
    shift = 0
    while True:
        byte = stream.read(1)
        if not byte:
            raise EOFError("Unexpected end of stream while reading uvarint")
        b = byte[0]
        value |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    return value


def decode_varint(stream: io.BufferedReader) -> int:
    """
    Reads a signed variable-length integer (ZigZag).
    Used for: Record length, deltas, key length, value length.
    """
    raw = decode_uvarint(stream)
    # Undo ZigZag: even numbers are positive, odd numbers are negative
    return (raw >> 1) ^ -(raw & 1)
```

---

## 7. Key Takeaways

1. **Protocol Distinctions**: Always verify if a field is documented as `VARINT` (ZigZag signed) or `UNSIGNED_VARINT` (standard unsigned).
2. **Storage Optimization**: ZigZag reduces small negative numbers (like `-1`) from 5 bytes down to a single byte.
3. **Bitwise Power**: Understanding bit shifts, masks, and two's complement enables building blazingly fast parsers, network proxies, and storage engines.
