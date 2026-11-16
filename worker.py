import socket
import threading
import time

import config
import protocol


def join(sock, server_addr):
    join_packet = protocol.pack(protocol.WORKER_JOIN, worker_id=0)
    sock.sendto(join_packet, server_addr)
    while True:
        data, _ = sock.recvfrom(config.MAX_PACKET)
        packet, reason = protocol.unpack(data)
        if packet is None:
            continue
        if packet["type"] == protocol.JOIN_ACK:
            return packet["worker_id"]


def heartbeat_thread(sock, server_addr, my_id, server_down):
    try:
        while True:
            heartbeat_packet = protocol.pack(protocol.HEARTBEAT, worker_id=my_id,
                                             payload=protocol.heartbeat_payload())
            sock.sendto(heartbeat_packet, server_addr)
            time.sleep(config.HEARTBEAT_INTERVAL)
    except ConnectionError:
        server_down.set()


def main():
    server_addr = (config.SERVER_HOST, config.SERVER_PORT)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_down = threading.Event()

    try:
        my_id = join(sock, server_addr)
        print("worker_id:", my_id)
        threading.Thread(target=heartbeat_thread,
                         args=(sock, server_addr, my_id, server_down),
                         daemon=True).start()
        sock.settimeout(config.RECV_TIMEOUT)

        while not server_down.is_set():
            try:
                data, _ = sock.recvfrom(config.MAX_PACKET)
            except socket.timeout:
                continue
            packet, reason = protocol.unpack(data)
            if packet is None:
                continue

            if packet["type"] == protocol.TASK_DISPATCH:
                start, end, difficulty = protocol.parse_dispatch(packet["payload"])
                result = protocol.solve(start, end, difficulty)
                result_packet = protocol.pack(
                    protocol.TASK_RESULT, task_id=packet["task_id"],
                    worker_id=my_id, attempt=packet["attempt"],
                    payload=protocol.result_payload(*result))
                sock.sendto(result_packet, server_addr)
    except ConnectionError:
        pass

    print("Server is not available")


if __name__ == "__main__":
    main()