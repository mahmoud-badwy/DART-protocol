import logging
import os
import socket
import threading
import time

import config
import protocol


def log_packet(direction, packet, addr, size, reason=None):
    if packet is None:
        logging.warning("%s/DROP | peer=%s | bytes=%s | reason=%s",
                        direction, addr, size, reason)
        return
    logging.info("%s | %s | peer=%s | worker=%s | task=%s | attempt=%s | bytes=%s",
                 direction, protocol.MSG_NAMES[packet["type"]], addr,
                 packet["worker_id"], packet["task_id"], packet["attempt"], size)


def send_packet(sock, data, server_addr):
    sock.sendto(data, server_addr)
    packet, reason = protocol.unpack(data)
    log_packet("SEND", packet, server_addr, len(data), reason)


def join(sock, server_addr):
    join_packet = protocol.pack(protocol.WORKER_JOIN, worker_id=0)
    send_packet(sock, join_packet, server_addr)
    while True:
        data, addr = sock.recvfrom(config.MAX_PACKET)
        packet, reason = protocol.unpack(data)
        log_packet("RECV", packet, addr, len(data), reason)
        if packet is None:
            continue
        if packet["type"] == protocol.JOIN_ACK:
            return packet["worker_id"]


def heartbeat_thread(sock, server_addr, my_id, server_down):
    try:
        while True:
            heartbeat_packet = protocol.pack(protocol.HEARTBEAT, worker_id=my_id,
                                             payload=protocol.heartbeat_payload())
            send_packet(sock, heartbeat_packet, server_addr)
            time.sleep(config.HEARTBEAT_INTERVAL)
    except ConnectionError as exc:
        logging.warning("HEARTBEAT_STOP | %s: %s", type(exc).__name__, exc)
        server_down.set()


def main():
    # The process ID gives each concurrently running worker its own log file.
    log_file = f"worker_{os.getpid()}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(log_file, mode="w", encoding="utf-8")],
    )
    server_addr = (config.SERVER_HOST, config.SERVER_PORT)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_down = threading.Event()
    logging.info("START | server=%s | log=%s", server_addr, log_file)

    try:
        my_id = join(sock, server_addr)
        logging.info("REGISTERED | worker=%s", my_id)
        threading.Thread(target=heartbeat_thread,
                         args=(sock, server_addr, my_id, server_down),
                         daemon=True).start()
        sock.settimeout(config.RECV_TIMEOUT)

        while not server_down.is_set():
            try:
                data, addr = sock.recvfrom(config.MAX_PACKET)
            except socket.timeout:
                continue
            packet, reason = protocol.unpack(data)
            log_packet("RECV", packet, addr, len(data), reason)
            if packet is None:
                continue

            if packet["type"] == protocol.TASK_DISPATCH:
                start, end, difficulty = protocol.parse_dispatch(packet["payload"])
                result = protocol.solve(start, end, difficulty)
                logging.info("TASK_DONE | worker=%s | task=%s | found=%s | nonce=%s",
                             my_id, packet["task_id"], result[0], result[1])
                result_packet = protocol.pack(
                    protocol.TASK_RESULT, task_id=packet["task_id"],
                    worker_id=my_id, attempt=packet["attempt"],
                    payload=protocol.result_payload(*result))
                send_packet(sock, result_packet, server_addr)
    except ConnectionError as exc:
        logging.warning("CONNECTION_ERROR | %s: %s", type(exc).__name__, exc)

    logging.warning("STOP | Server is not available")


if __name__ == "__main__":
    main()
