"""
rdt_common.py - packet format, checksum and an unreliable-network simulator.

Packet layout (big-endian, 11-byte header + payload):

    +--------+----------------+-----------+----------------+-----------------+
    | type   | seq (32 bit)   | len (16)  | CRC-32 (32)    | payload (<=1024)|
    | 1 byte | 4 bytes        | 2 bytes   | 4 bytes        |                 |
    +--------+----------------+-----------+----------------+-----------------+

The CRC covers the header (with the CRC field set to 0) plus the payload.

Packet types:
    SYN     sender -> receiver   connection request, payload = JSON metadata
    SYNACK  receiver -> sender   connection accepted
    DATA    sender -> receiver   one chunk of the file (seq = chunk index)
    ACK     receiver -> sender   GBN/SAW: seq = next expected chunk (cumulative)
                                 SR     : seq = the chunk being acknowledged
    FIN     sender -> receiver   all data acknowledged, close
    FINACK  receiver -> sender   close confirmed
"""
import heapq
import itertools
import random
import socket
import struct
import threading
import time
import zlib

HEADER = struct.Struct("!BIHI")
SYN, SYNACK, DATA, ACK, FIN, FINACK = range(6)
TYPE_NAMES = ["SYN", "SYNACK", "DATA", "ACK", "FIN", "FINACK"]
DEFAULT_PAYLOAD = 1024


def make_packet(ptype, seq, payload=b""):
    """Build a packet and fill in the CRC-32 checksum."""
    head = HEADER.pack(ptype, seq, len(payload), 0)
    crc = zlib.crc32(head + payload) & 0xFFFFFFFF
    return HEADER.pack(ptype, seq, len(payload), crc) + payload


def parse_packet(raw):
    """Return (type, seq, payload), or None if the packet is malformed/corrupt."""
    if len(raw) < HEADER.size:
        return None
    ptype, seq, length, crc = HEADER.unpack_from(raw)
    payload = raw[HEADER.size:]
    if ptype > FINACK or len(payload) != length:
        return None
    check = zlib.crc32(HEADER.pack(ptype, seq, length, 0) + payload) & 0xFFFFFFFF
    if check != crc:
        return None
    return ptype, seq, payload


class Channel:
    """
    A UDP socket wrapper that simulates a bad network on everything it SENDS:

        loss     probability a packet is silently dropped
        corrupt  probability one random bit of the packet is flipped
        delay    fixed one-way delay in seconds
        jitter   extra random delay in [0, jitter] seconds (causes reordering)
    """

    def __init__(self, sock, loss=0.0, corrupt=0.0, delay=0.0, jitter=0.0, seed=None):
        self.sock = sock
        self.loss, self.corrupt = loss, corrupt
        self.delay, self.jitter = delay, jitter
        self.rng = random.Random(seed)
        self.sent = self.dropped = self.corrupted = 0
        self._heap, self._ids = [], itertools.count()
        self._cv = threading.Condition()
        self._closed = False
        self._thread = None
        if delay > 0 or jitter > 0:
            self._thread = threading.Thread(target=self._pump, daemon=True)
            self._thread.start()

    # -- sending -----------------------------------------------------------
    def send(self, data, addr):
        self.sent += 1
        if self.rng.random() < self.loss:
            self.dropped += 1
            return
        if self.rng.random() < self.corrupt:
            buf = bytearray(data)
            buf[self.rng.randrange(len(buf))] ^= 1 << self.rng.randrange(8)
            data = bytes(buf)
            self.corrupted += 1
        if self._thread is None:
            self._raw_send(data, addr)
        else:
            due = time.monotonic() + self.delay + self.rng.uniform(0, self.jitter)
            with self._cv:
                heapq.heappush(self._heap, (due, next(self._ids), data, addr))
                self._cv.notify()

    def _raw_send(self, data, addr):
        try:
            self.sock.sendto(data, addr)
        except OSError:
            pass  # socket closed while a delayed packet was in flight

    def _pump(self):
        """Background thread that releases delayed packets when they are due."""
        while True:
            with self._cv:
                while True:
                    if self._closed:
                        return
                    if not self._heap:
                        self._cv.wait()
                        continue
                    wait = self._heap[0][0] - time.monotonic()
                    if wait <= 0:
                        break
                    self._cv.wait(wait)
                _, _, data, addr = heapq.heappop(self._heap)
            self._raw_send(data, addr)

    # -- receiving ---------------------------------------------------------
    def recv(self, timeout):
        """Wait up to `timeout` seconds. Returns (raw_bytes, addr) or None."""
        self.sock.settimeout(max(timeout, 0.001))
        try:
            return self.sock.recvfrom(65535)
        except (socket.timeout, BlockingIOError, ConnectionResetError):
            return None

    def close(self):
        with self._cv:
            self._closed = True
            self._cv.notify_all()
        self.sock.close()