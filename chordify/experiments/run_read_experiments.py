import os
import time
import argparse
import threading
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from net import send_request

lock = threading.Lock()


def worker(file_path, port, results, index, start_barrier):
    success_count = 0
    failure_count = 0

    start_barrier.wait()  # συγχρονισμένη εκκίνηση

    start = time.time()

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            key = line.strip()
            if not key:
                continue

            msg = {
                "type": "QUERY",
                "req_id": f"read_{index}_{success_count}",
                "origin": {"ip": "127.0.0.1", "port": 9999},
                "data": {"key": key},
            }

            response = send_request("127.0.0.1", port, msg)

            if response and response.get("status") == "OK":
                success_count += 1
            else:
                failure_count += 1

    end = time.time()

    with lock:
        results[index] = {
            "success": success_count,
            "fail": failure_count,
            "time": end - start,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=5000)
    parser.add_argument("--nodes", type=int, default=10)
    args = parser.parse_args()

    threads = []
    results = {}
    start_barrier = threading.Barrier(args.nodes)

    for n in range(args.nodes):
        file_path = os.path.join(
            os.path.dirname(__file__), "queries", f"query_{n:02d}.txt"
        )

        port = args.base_port + n

        t = threading.Thread(
            target=worker, args=(file_path, port, results, n, start_barrier)
        )

        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    total_success = sum(results[i]["success"] for i in results)
    total_fail = sum(results[i]["fail"] for i in results)
    max_time = max(results[i]["time"] for i in results)

    throughput = total_success / max_time if max_time > 0 else 0

    print("\n===== READ EXPERIMENT RESULTS =====")
    print(f"Successful queries: {total_success}")
    print(f"Failed queries: {total_fail}")
    print(f"Max completion time: {max_time:.4f} sec")
    print(f"System read throughput: {throughput:.4f} queries/sec")
    print("===================================\n")


if __name__ == "__main__":
    main()
