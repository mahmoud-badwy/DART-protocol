# DART Protocol

DART is a Phase 1 distributed task-processing prototype implemented in
Python. A UDP server divides a workload into tasks, assigns those tasks to
workers, and collects the results using a shared binary wire protocol.

## Repository

[View the project on GitHub](https://github.com/mahmoud-badwy/DART-protocol/s)

## Project explanation video

[Watch the project explanation video on Google Drive](https://drive.google.com/file/d/1Svv0bWmrG8HI6pwu7JPz3XJiA_O7oda4/view?usp=drive_link)

Make sure your Google account has permission to access the video.

## How it works

1. The server listens on `127.0.0.1:9000`.
2. Each worker sends a `WORKER_JOIN` message and receives a worker ID.
3. After four workers join, the server dispatches tasks to them.
4. A worker searches its assigned numeric range for a value whose SHA-256
   hexadecimal digest starts with the configured number of zeroes.
5. The worker sends a `TASK_RESULT`, and the server acknowledges it with an
   `ACK_RESULT`.
6. The server continues assigning tasks until all configured tasks are
   complete.

The default configuration contains 1,000 tasks, four workers, ranges of 5,000
numbers, and a difficulty of three leading hexadecimal zeroes.

## Repository structure

| File | Description |
| --- | --- |
| [`protocol.py`](protocol.py) | Packet format, CRC32 validation, payload helpers, task generation, and task-solving logic |
| [`config.py`](config.py) | Shared server, worker, timing, task, and packet-size configuration |
| [`server.py`](server.py) | UDP task coordinator |
| [`worker.py`](worker.py) | Worker process that joins the server, sends heartbeats, and solves tasks |
| [`run_baseline.py`](run_baseline.py) | Automated local benchmark and validation runner |

## Protocol overview

Each packet has a 16-byte big-endian header followed by an optional payload.
The header includes:

- Protocol version and message type
- Flags
- Task ID and worker ID
- Attempt number and fragmentation information
- Payload length
- CRC32 checksum

Supported message types are `WORKER_JOIN`, `JOIN_ACK`, `HEARTBEAT`,
`TASK_DISPATCH`, `TASK_RESULT`, and `ACK_RESULT`. Packets with an invalid
version, message type, length, or checksum are dropped.

## Requirements

- Python 3.8 or newer
- No third-party Python packages

## Running the prototype

Open separate terminals in the project directory.

### 1. Start the server

```bash
python server.py
```

### 2. Start four workers

```bash
python worker.py
```

Run the command in four separate terminals. The server starts dispatching
tasks after all four workers have registered.

The server writes to `server.log`. Each worker writes to a process-specific
`worker_<pid>.log` file.

## Running the baseline benchmark

The baseline runner starts the server and four workers automatically, validates
task dispatches and results, and stores its output under `baseline_results/`.

```bash
python run_baseline.py
```

Run one benchmark with a custom timeout:

```bash
python run_baseline.py --runs 1 --timeout 30
```

## Current Phase 1 limitations

- The prototype is intended for local execution.
- Packet retransmission and worker failure recovery are not implemented yet.
- Heartbeats are sent and logged, but Phase 1 does not perform failure
  detection based on them.
- The server expects four workers before beginning the workload.
