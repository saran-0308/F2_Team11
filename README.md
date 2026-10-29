# Reliable Data Transfer Protocol over UDP

## Project Description

This project implements reliable file transfer over UDP using three protocols: Stop-and-Wait, Go-Back-N, and Selective Repeat.

UDP does not guarantee that packets arrive successfully or in the correct order. This project implements reliability mechanisms such as acknowledgements, sequence numbers, checksums, timeouts, and retransmissions.

A simulated network introduces packet loss, corruption, delay, and packet reordering to evaluate protocol performance.

## Technologies Used

- Python 3
- UDP sockets
- CRC-32 checksum
- SHA-256 file verification
- Matplotlib for performance graphs

## Project Files

- `rdt_common.py` — Common packet handling and network simulation.
- `sender.py` — Sends file data using the selected protocol.
- `receiver.py` — Receives packets and reconstructs the file.
- `test_transfer.py` — Runs automated tests.
- `benchmark.py` — Runs performance experiments.
- `results.csv` — Stores benchmark measurements.
- `graph_loss_comparison.png` — Compares performance under packet-loss conditions.
- `graph_window_size.png` — Shows the effect of window size.

## Requirements

- Python 3
- Matplotlib, if required by the benchmark scripts

Install dependencies using:

```bash
python3 -m pip install -r requirements.txt
```

## How to Run

### 1. Run the tests

```bash
python3 test_transfer.py
```

### 2. Start the receiver

Open a terminal and run:

```bash
python3 receiver.py --port 9000 --out received.bin
```

Keep the receiver running.

### 3. Start the sender

Open a second terminal and run:

```bash
python3 sender.py sample.bin --port 9000 --mode sr --window 8 --timeout 0.05 --loss 0.1
```

This example uses Selective Repeat with a configured packet-loss rate of 10%.

### 4. Verify the transferred file

After the transfer completes, run:

```bash
shasum -a 256 sample.bin received.bin
```

The hashes should match if the files are identical.

### 5. Run the benchmark

```bash
python3 benchmark.py
```

Check the script's output for the generated CSV and graphs.

## Evaluation

The project evaluates:

- Successful file transfer
- File integrity
- Transfer time
- Throughput
- Retransmissions
- Effects of packet loss and window size

## Conclusion

This project demonstrates how reliable data transfer can be implemented over UDP and compares Stop-and-Wait, Go-Back-N, and Selective Repeat under simulated network conditions.