"""
sender.py - reliable file sender over UDP.

Modes:
    saw  Stop-and-Wait    (window = 1)
    gbn  Go-Back-N        (cumulative ACKs, one timer, resend whole window)
    sr   Selective Repeat (individual ACKs, one timer per packet)

Usage:
    python sender.py FILE --host 127.0.0.1 --port 9000 --mode sr --window 8
"""
import argparse
import hashlib
import json
import os
import socket
import time

from rdt_common import (ACK, DATA, DEFAULT_PAYLOAD, FIN, FINACK, SYN, SYNACK,
                        Channel, make_packet, parse_packet)


def _wait_for(chan, ptype, timeout):
    """Wait up to `timeout` seconds for a valid packet of the given type."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        r = chan.recv(remaining)
        if r:
            pkt = parse_packet(r[0])
            if pkt and pkt[0] == ptype:
                return True


def send_file(path, addr, chan, mode="sr", window=8, timeout=0.2,
              payload_size=DEFAULT_PAYLOAD, log=None, max_idle=15.0):
    log = log or (lambda msg: None)
    mode = mode.lower()
    if mode not in ("saw", "gbn", "sr"):
        raise ValueError("mode must be saw, gbn or sr")
    if mode == "saw":
        window = 1

    with open(path, "rb") as f:
        data = f.read()
    chunks = [data[i:i + payload_size] for i in range(0, len(data), payload_size)] or [b""]
    total = len(chunks)
    packets = [make_packet(DATA, i, c) for i, c in enumerate(chunks)]
    st = {"data_sent": 0, "retransmissions": 0, "timeouts": 0,
          "acks_received": 0, "corrupt_acks": 0}

    # ---- connection setup (SYN / SYNACK) ----------------------------------
    meta = json.dumps({"name": os.path.basename(path), "size": len(data),
                       "total": total, "mode": mode, "window": window}).encode()
    syn = make_packet(SYN, 0, meta)
    for _ in range(20):
        log("Sent SYN")
        chan.send(syn, addr)
        if _wait_for(chan, SYNACK, timeout):
            log("Received SYNACK - connection established")
            break
        log("SYN timeout, retrying")
    else:
        raise ConnectionError("receiver did not answer the SYN")

    def send(seq, retrans=False):
        chan.send(packets[seq], addr)
        st["data_sent"] += 1
        if retrans:
            st["retransmissions"] += 1
            log(f"Timeout, retransmitting seq={seq}")
        else:
            log(f"Sent packet seq={seq}")

    def read_ack():
        """Return ACK seq number, or None (nothing / corrupt / not an ACK)."""
        r = chan.recv(read_ack.wait)
        if not r:
            return None
        pkt = parse_packet(r[0])
        if pkt is None:
            st["corrupt_acks"] += 1
            log("Corrupted ACK discarded")
            return None
        if pkt[0] != ACK:
            return None
        st["acks_received"] += 1
        log(f"ACK received for {pkt[1]}")
        return pkt[1]

    # ---- data transfer ------------------------------------------------------
    t0 = time.monotonic()
    last_progress = t0
    base = nxt = 0

    if mode in ("saw", "gbn"):
        timer = None
        while base < total:
            while nxt < min(base + window, total):
                send(nxt)
                if base == nxt:
                    timer = time.monotonic() + timeout
                nxt += 1
            read_ack.wait = max(0.0, (timer or 0) - time.monotonic())
            a = read_ack()                       # cumulative: a = next expected
            if a is not None and base < a <= nxt:
                base = a
                last_progress = time.monotonic()
                timer = time.monotonic() + timeout if base < nxt else None
            if timer is not None and time.monotonic() >= timer:
                st["timeouts"] += 1
                for s in range(base, nxt):       # go back N: resend everything
                    send(s, retrans=True)
                timer = time.monotonic() + timeout
            if time.monotonic() - last_progress > max_idle:
                raise TimeoutError("no progress - receiver unreachable?")
    else:  # Selective Repeat
        acked = [False] * total
        sent_at = {}                             # seq -> time of last send (unacked only)
        while base < total:
            while nxt < min(base + window, total):
                send(nxt)
                sent_at[nxt] = time.monotonic()
                nxt += 1
            nearest = min(sent_at.values()) + timeout if sent_at else time.monotonic() + timeout
            read_ack.wait = max(0.0, nearest - time.monotonic())
            s = read_ack()                       # individual ACK for packet s
            if s is not None and base <= s < nxt and not acked[s]:
                acked[s] = True
                sent_at.pop(s, None)
                last_progress = time.monotonic()
                while base < total and acked[base]:
                    base += 1
            now = time.monotonic()
            for seq, t in list(sent_at.items()):  # per-packet timers
                if now - t >= timeout:
                    st["timeouts"] += 1
                    send(seq, retrans=True)
                    sent_at[seq] = now
            if now - last_progress > max_idle:
                raise TimeoutError("no progress - receiver unreachable?")

    elapsed = time.monotonic() - t0

    # ---- connection teardown (FIN / FINACK) ------------------------------------
    fin = make_packet(FIN, total)
    for _ in range(8):
        chan.send(fin, addr)
        log("Sent FIN")
        if _wait_for(chan, FINACK, timeout):
            log("Received FINACK - transfer complete")
            break

    st.update(mode=mode, window=window, packets=total, bytes=len(data),
              elapsed=elapsed, throughput_kBps=len(data) / 1024 / max(elapsed, 1e-9),
              sha256=hashlib.sha256(data).hexdigest(),
              net_sent=chan.sent, net_dropped=chan.dropped, net_corrupted=chan.corrupted)
    return st


def main():
    p = argparse.ArgumentParser(description="Reliable UDP file sender")
    p.add_argument("file")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9000)
    p.add_argument("--mode", choices=["saw", "gbn", "sr"], default="sr")
    p.add_argument("--window", type=int, default=8)
    p.add_argument("--timeout", type=float, default=0.2, help="retransmission timeout (s)")
    p.add_argument("--loss", type=float, default=0.0, help="simulated loss probability")
    p.add_argument("--corrupt", type=float, default=0.0, help="simulated corruption probability")
    p.add_argument("--delay", type=float, default=0.0, help="simulated one-way delay (s)")
    p.add_argument("--jitter", type=float, default=0.0, help="random extra delay (s), causes reordering")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--verbose", action="store_true")
    a = p.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("", 0))
    chan = Channel(sock, a.loss, a.corrupt, a.delay, a.jitter, a.seed)
    log = (lambda m: print("[sender]", m)) if a.verbose else None
    st = send_file(a.file, (a.host, a.port), chan, a.mode, a.window, a.timeout, log=log)
    chan.close()

    print("\n=== Sender summary ===")
    print(f"Mode               : {st['mode'].upper()} (window={st['window']})")
    print(f"File               : {st['bytes']} bytes in {st['packets']} packets")
    print(f"Data packets sent  : {st['data_sent']}")
    print(f"Retransmissions    : {st['retransmissions']}  (timeouts: {st['timeouts']})")
    print(f"ACKs received      : {st['acks_received']}  (corrupt ACKs: {st['corrupt_acks']})")
    print(f"Simulated network  : {st['net_dropped']} dropped, {st['net_corrupted']} corrupted of {st['net_sent']} sent")
    print(f"Transfer time      : {st['elapsed']:.3f} s")
    print(f"Throughput         : {st['throughput_kBps']:.1f} KB/s")
    print(f"SHA-256            : {st['sha256']}")


if __name__ == "__main__":
    main()