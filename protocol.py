"""DART v1.0 wire format: 16-byte header + payload, all big-endian.

Offset Size Field
  0     1   Version(4 bits) | MsgType(4 bits)
  1     1   Flags (reserved, 0)
  2     4   Task_ID
  6     2   Worker_ID
  8     1   Attempt_Num
  9     1   Frag_Index(4 bits) | Frag_Count(4 bits)
 10     2   Payload_Len
 12     4   Checksum = CRC32(header-with-checksum-zeroed + payload)
"""
import struct
import time
import zlib

from config import MAX_PACKET

VERSION     = 1
HEADER_FMT  = "!BBIHBBHI"
HEADER_SIZE = struct.calcsize(HEADER_FMT)      # 16
MAX_PAYLOAD = MAX_PACKET - HEADER_SIZE         # 496

(WORKER_JOIN, JOIN_ACK, HEARTBEAT,
 TASK_DISPATCH, TASK_RESULT, ACK_RESULT) = range(6)
MSG_NAMES = ["WORKER_JOIN", "JOIN_ACK", "HEARTBEAT",
             "TASK_DISPATCH", "TASK_RESULT", "ACK_RESULT"]

HEARTBEAT_FMT = "!Q"     # timestamp, microseconds
DISPATCH_FMT  = "!QQB"   # range_start, range_end, difficulty
RESULT_FMT    = "!BQ"    # found, nonce

assert HEADER_SIZE == 16


def now_us():
    """Wall-clock microseconds - for logs/pcap correlation ONLY.
    Use time.monotonic() for timeouts."""
    return time.time_ns() // 1000


def _header(vt, flags, task_id, worker_id, attempt, frag, plen, crc):
    return struct.pack(HEADER_FMT, vt, flags, task_id, worker_id,
                       attempt, frag, plen, crc)


def pack(msg_type, task_id=0, worker_id=0, attempt=0, payload=b"",
         flags=0, frag_index=0, frag_count=1):
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("payload too large")
    vt   = (VERSION << 4) | (msg_type & 0x0F)
    frag = ((frag_index & 0x0F) << 4) | (frag_count & 0x0F)
    hdr  = _header(vt, flags, task_id, worker_id, attempt, frag, len(payload), 0)
    crc  = zlib.crc32(hdr + payload) & 0xFFFFFFFF
    hdr  = _header(vt, flags, task_id, worker_id, attempt, frag, len(payload), crc)
    return hdr + payload


def unpack(data):
    """-> (packet_dict, None) if valid, or (None, reason) if it must be dropped."""
    if len(data) < HEADER_SIZE:
        return None, "too_short"
    vt, flags, task_id, worker_id, attempt, frag, plen, crc = \
        struct.unpack(HEADER_FMT, data[:HEADER_SIZE])
    version, msg_type = vt >> 4, vt & 0x0F
    payload = data[HEADER_SIZE:]
    if version != VERSION:
        return None, "bad_version"
    if msg_type > ACK_RESULT:
        return None, "bad_type"
    if plen != len(payload):
        return None, "bad_length"
    zero = _header(vt, flags, task_id, worker_id, attempt, frag, plen, 0)
    if zlib.crc32(zero + payload) & 0xFFFFFFFF != crc:
        return None, "bad_checksum"
    return {"type": msg_type, "flags": flags, "task_id": task_id,
            "worker_id": worker_id, "attempt": attempt,
            "frag_index": frag >> 4, "frag_count": frag & 0x0F,
            "payload": payload}, None


# ---- payload helpers -------------------------------------------------------
def dispatch_payload(start, end, difficulty):
    return struct.pack(DISPATCH_FMT, start, end, difficulty)

def parse_dispatch(p):
    return struct.unpack(DISPATCH_FMT, p)       # (start, end, difficulty)

def result_payload(found, nonce):
    return struct.pack(RESULT_FMT, found, nonce)

def parse_result(p):
    return struct.unpack(RESULT_FMT, p)         # (found, nonce)

def heartbeat_payload():
    return struct.pack(HEARTBEAT_FMT, now_us())

def parse_heartbeat(p):
    return struct.unpack(HEARTBEAT_FMT, p)[0]


# ---- the task itself (shared so server & worker agree) ---------------------
def task_range(task_id, range_size):
    return task_id * range_size, (task_id + 1) * range_size

def solve(start, end, difficulty):
    import hashlib
    prefix = "0" * difficulty
    for n in range(start, end):
        if hashlib.sha256(str(n).encode()).hexdigest().startswith(prefix):
            return 1, n
    return 0, 0
