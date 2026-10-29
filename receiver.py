"""
receiver.py - reliable file receiver over UDP.

Usage:
    python receiver.py --port 9000 --out received.bin
"""
import argparse
import hashlib
import json
import socket
import time

from rdt_common import (ACK, DATA, FIN, FINACK, SYN, SYNACK,
                        Channel, make_packet, parse_packet)


def receive_file(chan, out_path, idle_timeout=30.0, linger=1.0, log=None):
    """
    Receive one file. Returns a stats dict.

    `linger` = how long to keep answering after the FIN, so that a lost
    FINACK (or lost final ACK) can still be repaired by the sender.
    """
    log = log or (lambda msg: None)
    st = {"delivered": 0, "duplicates": 0, "corrupt": 0, "out_of_order": 0,
          "buffered": 0, "acks_sent": 0}
    meta = None
    out = None
    expected = 0
    buf = {}                      # SR out-of-order buffer: seq -> payload
    last_activity = time.monotonic()
    fin_time = None

    while True:
        now = time.monotonic()
        if fin_time is not None and now - fin_time > linger:
            break
        if fin_time is None and now - last_activity > idle_timeout:
            raise TimeoutError("receiver idle for too long")

        r = chan.recv(0.1)
        if r is None:
            continue
        raw, addr = r
        pkt = parse_packet(raw)
        if pkt is None:
            st["corrupt"] += 1
            log("Corrupted packet detected, discarded")
            continue
        ptype, seq, payload = pkt
        last_activity = time.monotonic()

        def ack(n):
            chan.send(make_packet(ACK, n), addr)
            st["acks_sent"] += 1

        if ptype == SYN:
            if meta is None:
                meta = json.loads(payload)
                out = open(out_path, "wb")
                log(f"SYN received: {meta['name']} ({meta['size']} bytes, "
                    f"{meta['total']} packets, mode={meta['mode']}, window={meta['window']})")
            chan.send(make_packet(SYNACK, 0), addr)

        elif meta is None:
            continue                       # ignore anything before the handshake

        elif ptype == DATA:
            if seq >= meta["total"]:
                continue
            if meta["mode"] in ("saw", "gbn"):
                # Go-Back-N receiver: accept only the next expected packet
                if seq == expected:
                    out.write(payload)
                    expected += 1
                    st["delivered"] += 1
                    log(f"Delivered seq={seq}")
                elif seq < expected:
                    st["duplicates"] += 1
                    log(f"Duplicate packet seq={seq} discarded")
                else:
                    st["out_of_order"] += 1
                    log(f"Out-of-order seq={seq} discarded (expected {expected})")
                ack(expected)              # cumulative ACK
            else:
                # Selective Repeat receiver: buffer out-of-order packets
                w = meta["window"]
                if seq < expected:
                    st["duplicates"] += 1
                    log(f"Duplicate packet seq={seq} (re-ACKed)")
                    ack(seq)
                elif seq < expected + w:
                    if seq in buf:
                        st["duplicates"] += 1
                        log(f"Duplicate packet seq={seq} (re-ACKed)")
                    else:
                        buf[seq] = payload
                        if seq != expected:
                            st["out_of_order"] += 1
                            st["buffered"] += 1
                            log(f"Buffered out-of-order seq={seq} (expected {expected})")
                    ack(seq)               # individual ACK
                    while expected in buf:
                        out.write(buf.pop(expected))
                        st["delivered"] += 1
                        log(f"Delivered seq={expected}")
                        expected += 1

        elif ptype == FIN:
            if expected >= meta["total"]:
                chan.send(make_packet(FINACK, seq), addr)
                fin_time = time.monotonic()
                log("FIN received, FINACK sent")

    out.close()
    with open(out_path, "rb") as f:
        digest = hashlib.sha256(f.read()).hexdigest()
    st.update(sha256=digest, expected_sha256=None, total=meta["total"], size=meta["size"])
    return st


def main():
    p = argparse.ArgumentParser(description="Reliable UDP file receiver")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--out", default="received.bin")
    p.add_argument("--loss", type=float, default=0.0, help="simulated loss on ACKs")
    p.add_argument("--corrupt", type=float, default=0.0)
    p.add_argument("--delay", type=float, default=0.0)
    p.add_argument("--jitter", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--verbose", action="store_true")
    a = p.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", a.port))
    chan = Channel(sock, a.loss, a.corrupt, a.delay, a.jitter, a.seed)
    print(f"[receiver] listening on UDP port {a.port} ...")
    log = (lambda m: print("[receiver]", m)) if a.verbose else None
    st = receive_file(chan, a.out, log=log)
    chan.close()

    print("\n=== Receiver summary ===")
    print(f"Saved to           : {a.out} ({st['size']} bytes, {st['total']} packets)")
    print(f"Delivered in order : {st['delivered']}")
    print(f"Duplicates dropped : {st['duplicates']}")
    print(f"Out-of-order       : {st['out_of_order']}  (buffered: {st['buffered']})")
    print(f"Corrupted dropped  : {st['corrupt']}")
    print(f"SHA-256            : {st['sha256']}")


if __name__ == "__main__":
    main()