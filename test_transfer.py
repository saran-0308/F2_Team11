"""Correctness test: every mode must deliver a byte-identical file, even on a bad network."""
import hashlib, os, socket, sys, threading
from rdt_common import Channel
from receiver import receive_file
from sender import send_file

SRC, DST = "test_input.bin", "test_output.bin"


def run(mode, window, **net):
    rs = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); rs.bind(("127.0.0.1", 0))
    ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); ss.bind(("127.0.0.1", 0))
    rc, sc = Channel(rs, seed=1, **net), Channel(ss, seed=2, **net)
    res = {}
    t = threading.Thread(target=lambda: res.update(r=receive_file(rc, DST, linger=0.3)))
    t.start()
    st = send_file(SRC, rs.getsockname(), sc, mode, window, timeout=0.05)
    t.join()
    ok = hashlib.sha256(open(DST, "rb").read()).hexdigest() == st["sha256"]
    r = res["r"]
    print(f"{mode.upper():3} w={window:<2} {str(net) or 'clean':<75} match={ok}  "
          f"time={st['elapsed']:.2f}s retx={st['retransmissions']} dup={r['duplicates']} "
          f"ooo={r['out_of_order']} corrupt={r['corrupt']}")
    rc.close(); sc.close()
    return ok


if __name__ == "__main__":
    with open(SRC, "wb") as f:
        f.write(os.urandom(60000))
    bad = dict(loss=0.15, corrupt=0.05, delay=0.005, jitter=0.01)  # loss + corruption + reordering
    ok = all([run(m, w, **n) for m, w in (("saw", 1), ("gbn", 8), ("sr", 8)) for n in ({}, bad)])
    os.remove(SRC); os.remove(DST)
    print("\nALL TESTS PASSED" if ok else "\nSOME TESTS FAILED")
    sys.exit(0 if ok else 1)