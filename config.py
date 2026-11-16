"""DART v1.0 - all shared constants live here (single source of truth)."""
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 9000

NUM_TASKS  = 1000
RANGE_SIZE = 5000          # task i covers [i*RANGE_SIZE, (i+1)*RANGE_SIZE)
DIFFICULTY = 3             # leading hex zeros; tune so one task ~5-20 ms

HEARTBEAT_INTERVAL = 0.200 # s, worker -> server
WORKER_DEAD_AFTER  = 1.000 # s without any sign of life
TASK_TIMEOUT       = 0.500 # s, DISPATCHED with no result -> reassign
RESULT_RTO_INITIAL = 0.300 # s, first wait for ACK_RESULT, doubles each retry
RESULT_MAX_TRIES   = 5
RECV_TIMEOUT       = 0.020 # s, loop tick

MAX_PACKET = 512           # bytes, header + payload
