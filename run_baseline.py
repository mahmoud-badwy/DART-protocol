"""Run beside server.py, worker.py, config.py and protocol.py.

Usage: python run_baseline.py
Default: five local runs, four workers, 60-second timeout per run.
Optional: python run_baseline.py --runs 1 --timeout 30
Results and separate process logs are saved under baseline_results/.
Only processes started by this script are stopped after each run.
Worker console capture does not replace packet logging inside worker.py.
"""

import argparse
import csv
from datetime import datetime
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time
import uuid

import config


PROJECT = Path(__file__).resolve().parent
WORKERS = 4


def read_log(path):
    return path.read_text(encoding="utf-8", errors="replace") if path.exists() else ""


def stop(processes):
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def run_once(folder, timeout):
    processes, handles = [], []
    console = folder / "server" / "console.log"
    deadline = time.monotonic() + timeout

    def launch(filename, name):
        workdir = folder / name
        workdir.mkdir(parents=True)
        output = (workdir / "console.log").open("w", encoding="utf-8")
        handles.append(output)
        process = subprocess.Popen(
            [sys.executable, "-u", str(PROJECT / filename)],
            cwd=workdir, stdout=output, stderr=subprocess.STDOUT,
        )
        processes.append(process)
        return process

    try:
        server = launch("server.py", "server")
        while "LISTEN |" not in read_log(console):
            if server.poll() is not None:
                raise RuntimeError("Server failed to start; see server/console.log")
            if time.monotonic() >= deadline:
                raise RuntimeError("Timed out waiting for server startup")
            time.sleep(0.05)

        workers = [launch("worker.py", f"worker_{i}") for i in range(1, WORKERS + 1)]
        while server.poll() is None:
            text = read_log(console)
            if any(worker.poll() is not None for worker in workers) and "COMPLETE |" not in text:
                raise RuntimeError("A worker exited before completion; see its console.log")
            if time.monotonic() >= deadline:
                raise RuntimeError("Baseline timed out before server completion")
            time.sleep(0.05)

        if server.returncode != 0:
            raise RuntimeError("Server exited with an error; see server/console.log")
        text = read_log(console)
        summary = re.search(
            r"COMPLETE \| tasks=(\d+) \| workers=(\d+) \| elapsed_seconds=([\d.]+)", text
        )
        accepted = re.findall(r"\| RESULT \| task=(\d+) \| worker=(\d+) \|", text)
        dispatched = re.findall(r"SEND \| TASK_DISPATCH .*?\| task=(\d+) \|", text)
        acknowledgments = re.findall(r"SEND \| ACK_RESULT .*?\| task=(\d+) \|", text)
        expected = set(range(config.NUM_TASKS))
        for label, ids in (
            ("accepted results", [int(task) for task, _ in accepted]),
            ("dispatches", [int(task) for task in dispatched]),
            ("result acknowledgments", [int(task) for task in acknowledgments]),
        ):
            if len(ids) != config.NUM_TASKS or set(ids) != expected:
                raise RuntimeError(f"Missing or repeated task IDs in {label}")
        if not summary or int(summary[1]) != config.NUM_TASKS or int(summary[2]) != WORKERS:
            raise RuntimeError("Missing or incorrect completion summary")
        if config.NUM_TASKS >= WORKERS and {int(w) for _, w in accepted} != set(range(1, WORKERS + 1)):
            raise RuntimeError("Not all four workers returned results")
        return float(summary[3])
    finally:
        # The supplied worker keeps waiting after the server finishes.
        stop(processes)
        for output in handles:
            output.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=60)
    args = parser.parse_args()
    if args.runs < 1 or args.timeout <= 0:
        parser.error("--runs and --timeout must be positive")
    for name in ("server.py", "worker.py", "config.py", "protocol.py"):
        if not (PROJECT / name).is_file():
            parser.error(f"Missing {name}; place this script beside the four project files")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = PROJECT / "baseline_results" / f"{stamp}_{uuid.uuid4().hex[:8]}"
    results.mkdir(parents=True)
    print(f"Results: {results}", flush=True)
    times = []
    with (results / "summary.csv").open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(["run", "status", "tasks", "workers", "server_elapsed_seconds", "error"])
        for run in range(1, args.runs + 1):
            folder = results / f"run_{run:02d}"
            try:
                elapsed = run_once(folder, args.timeout)
            except (OSError, RuntimeError) as error:
                writer.writerow([run, "FAIL", config.NUM_TASKS, WORKERS, "", str(error)])
                print(f"Run {run}: FAIL - {error}", file=sys.stderr)
                return 1
            except KeyboardInterrupt:
                writer.writerow([run, "INTERRUPTED", config.NUM_TASKS, WORKERS, "", "Ctrl+C"])
                print("Stopped. Child processes cleaned up.", file=sys.stderr)
                return 130
            times.append(elapsed)
            writer.writerow([run, "PASS", config.NUM_TASKS, WORKERS, elapsed, ""])
            output.flush()
            print(f"Run {run}: PASS | {config.NUM_TASKS} tasks | {elapsed:.3f} s", flush=True)
    stats = f"Server time (s): min={min(times):.3f}, median={statistics.median(times):.3f}, max={max(times):.3f}"
    (results / "statistics.txt").write_text(stats + "\n", encoding="utf-8")
    print(stats)
    print("PASS checks task IDs, dispatches, accepted results and ACKs; it does not verify computation or packet-loss recovery.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
