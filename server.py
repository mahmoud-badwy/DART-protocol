"""DART Phase 1 local prototype: run this server, then four worker.py processes.

Logs are written to server.log. Stop the workers with Ctrl+C after completion.
Packet retransmission and worker failure recovery belong to the next phase.
"""
import logging
import socket
import struct
import time

import config
import protocol

WORKER_COUNT = 4


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler("server.log", mode="w")],
    )
    workers = {}       # (IP, port) -> worker ID
    busy = {}          # worker ID -> current task ID
    next_task = 0
    finished = 0
    started_at = None

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((config.SERVER_HOST, config.SERVER_PORT))
        logging.info("LISTEN | %s:%s | waiting for %s workers",
                     config.SERVER_HOST, config.SERVER_PORT, WORKER_COUNT)

        def send(kind, addr, worker_id, task_id=0, attempt=0, payload=b""):
            data = protocol.pack(kind, task_id=task_id, worker_id=worker_id,
                                 attempt=attempt, payload=payload)
            sock.sendto(data, addr)
            logging.info("SEND | %s | peer=%s | worker=%s | task=%s | bytes=%s",
                         protocol.MSG_NAMES[kind], addr, worker_id, task_id, len(data))

        def dispatch(addr, worker_id):
            nonlocal next_task
            if next_task < config.NUM_TASKS and worker_id not in busy:
                start, end = protocol.task_range(next_task, config.RANGE_SIZE)
                payload = protocol.dispatch_payload(start, end, config.DIFFICULTY)
                send(protocol.TASK_DISPATCH, addr, worker_id, next_task, 1, payload)
                busy[worker_id] = next_task
                next_task += 1

        while finished < config.NUM_TASKS:
            data, addr = sock.recvfrom(65535)
            if len(data) > config.MAX_PACKET:
                logging.warning("RECV/DROP | peer=%s | bytes=%s | oversized", addr, len(data))
                continue
            packet, reason = protocol.unpack(data)
            if packet is None:
                logging.warning("RECV/DROP | peer=%s | reason=%s", addr, reason)
                continue
            kind = packet["type"]
            worker_id = packet["worker_id"]
            task_id = packet["task_id"]
            logging.info("RECV | %s | peer=%s | worker=%s | task=%s | bytes=%s",
                         protocol.MSG_NAMES[kind], addr, worker_id, task_id, len(data))

            if kind == protocol.WORKER_JOIN:
                if addr not in workers:
                    if len(workers) == WORKER_COUNT:
                        continue
                    workers[addr] = len(workers) + 1
                send(protocol.JOIN_ACK, addr, workers[addr])
                if len(workers) == WORKER_COUNT and started_at is None:
                    started_at = time.monotonic()
                    for worker_addr, registered_id in workers.items():
                        dispatch(worker_addr, registered_id)

            elif workers.get(addr) != worker_id:
                logging.warning("DROP | unknown worker")

            elif kind == protocol.HEARTBEAT:
                pass  # Receipt is logged above; no failure detection in Phase 1.

            elif kind == protocol.TASK_RESULT:
                if busy.get(worker_id) != task_id or packet["attempt"] != 1:
                    logging.warning("DROP | result does not match the assigned task")
                    continue
                try:
                    found, nonce = protocol.parse_result(packet["payload"])
                except struct.error:
                    logging.warning("DROP | invalid result payload")
                    continue
                del busy[worker_id]
                finished += 1
                logging.info("RESULT | task=%s | worker=%s | found=%s | nonce=%s | completed=%s/%s",
                             task_id, worker_id, found, nonce, finished, config.NUM_TASKS)
                send(protocol.ACK_RESULT, addr, worker_id, task_id, 1)
                dispatch(addr, worker_id)

        logging.info("COMPLETE | tasks=%s | workers=%s | elapsed_seconds=%.3f",
                     finished, len(workers), time.monotonic() - started_at)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("STOP | interrupted by user")
