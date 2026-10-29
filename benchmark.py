"""
benchmark.py - runs the experiments for the report and draws the graphs.

Experiment 1: Stop-and-Wait vs Go-Back-N vs Selective Repeat, loss 0..20 %
Experiment 2: effect of window size (GBN vs SR) at 5 % loss

Every run transfers a random file between two threads over real UDP sockets
on localhost, through the lossy Channel (5 ms one-way delay, no reordering),
and checks that the SHA-256 of the received file equals the original.

Usage:  python benchmark.py [--size-kb 100] [--repeat 1]
"""
import argparse
import csv
import hashlib
import os
import socket
import threading

from rdt_common import Channel
from receiver import receive_file
from sender import send_file

DELAY, JITTER, TIMEOUT = 0.005, 0.0, 0.06


def run_once(path, mode, window, loss, seed):
    rs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rs.bind(("127.0.0.1", 0))
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ss.bind(("127.0.0.1", 0))
    net = dict(loss=loss, delay=DELAY, jitter=JITTER)
    rc, sc = Channel(rs, seed=seed, **net), Channel(ss, seed=seed + 1000, **net)
    out = f"bench_out_{os.getpid()}.bin"
    holder = {}
    t = threading.Thread(target=lambda: holder.update(
        r=receive_file(rc, out, linger=0.3)))
    t.start()
    st = send_file(path, rs.getsockname(), sc, mode, window, TIMEOUT)
    t.join()
    with open(out, "rb") as f:
        ok = hashlib.sha256(f.read()).hexdigest() == st["sha256"]
    os.remove(out)
    rc.close()
    sc.close()
    assert ok, f"FILE MISMATCH ({mode}, window={window}, loss={loss})"
    return st


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--size-kb", type=int, default=100)
    ap.add_argument("--repeat", type=int, default=1)
    a = ap.parse_args()

    path = "bench_input.bin"
    with open(path, "wb") as f:
        f.write(os.urandom(a.size_kb * 1024))

    rows = []

    def record(exp, mode, window, loss):
        times, thr, ret = [], [], []
        for r in range(a.repeat):
            st = run_once(path, mode, window, loss, seed=r * 7 + int(loss * 100) + window)
            times.append(st["elapsed"])
            thr.append(st["throughput_kBps"])
            ret.append(st["retransmissions"])
        n = len(times)
        row = dict(experiment=exp, mode=mode, window=window, loss_pct=int(loss * 100),
                   time_s=round(sum(times) / n, 3), throughput_kBps=round(sum(thr) / n, 1),
                   retransmissions=round(sum(ret) / n, 1))
        rows.append(row)
        print(row, flush=True)

    # Experiment 1: protocol comparison vs loss
    for loss in (0.0, 0.05, 0.10, 0.20):
        for mode, w in (("saw", 1), ("gbn", 8), ("sr", 8)):
            record(1, mode, w, loss)

    # Experiment 2: window size at 5 % loss
    for w in (1, 2, 4, 8, 16, 32):
        for mode in ("gbn", "sr"):
            record(2, mode, w, 0.05)

    with open("results.csv", "w", newline="") as f:
        wr = csv.DictWriter(f, fieldnames=list(rows[0]))
        wr.writeheader()
        wr.writerows(rows)

    # ---- graphs ------------------------------------------------------------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    names = {"saw": "Stop-and-Wait", "gbn": "Go-Back-N (w=8)", "sr": "Selective Repeat (w=8)"}
    e1 = [r for r in rows if r["experiment"] == 1]
    fig, ax = plt.subplots(1, 3, figsize=(15, 4.2))
    for mode in ("saw", "gbn", "sr"):
        pts = [r for r in e1 if r["mode"] == mode]
        x = [r["loss_pct"] for r in pts]
        ax[0].plot(x, [r["throughput_kBps"] for r in pts], "o-", label=names[mode])
        ax[1].plot(x, [r["retransmissions"] for r in pts], "o-", label=names[mode])
        ax[2].plot(x, [r["time_s"] for r in pts], "o-", label=names[mode])
    for axis, title, yl in zip(ax, ("Throughput vs packet loss", "Retransmissions vs packet loss",
                                    "Transfer time vs packet loss"),
                               ("Throughput (KB/s)", "Retransmitted packets", "Time (s)")):
        axis.set_title(title)
        axis.set_xlabel("Packet loss (%)")
        axis.set_ylabel(yl)
        axis.grid(alpha=0.3)
        axis.legend()
    plt.tight_layout()
    plt.savefig("graph_loss_comparison.png", dpi=140)

    e2 = [r for r in rows if r["experiment"] == 2]
    fig, ax = plt.subplots(1, 2, figsize=(10, 4.2))
    for mode in ("gbn", "sr"):
        pts = [r for r in e2 if r["mode"] == mode]
        ax[0].plot([r["window"] for r in pts], [r["time_s"] for r in pts], "o-", label=mode.upper())
        ax[1].plot([r["window"] for r in pts], [r["retransmissions"] for r in pts], "o-", label=mode.upper())
    for axis, title, yl in zip(ax, ("Transfer time vs window size (5% loss)",
                                    "Retransmissions vs window size (5% loss)"),
                               ("Time (s)", "Retransmitted packets")):
        axis.set_title(title)
        axis.set_xlabel("Window size")
        axis.set_xscale("log", base=2)
        axis.set_ylabel(yl)
        axis.grid(alpha=0.3)
        axis.legend()
    plt.tight_layout()
    plt.savefig("graph_window_size.png", dpi=140)
    os.remove(path)
    print("\nSaved results.csv, graph_loss_comparison.png, graph_window_size.png")


if __name__ == "__main__":
    main()