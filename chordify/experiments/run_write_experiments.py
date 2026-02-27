import os
import time
import argparse
import threading
import sys

# Προσθέτουμε το src στο path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from net import send_request


def worker(file_path, port, value, results, index):
    start = time.time()
    count = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            song = line.strip()
            if not song:
                continue

            msg = {
                "type": "INSERT",
                "req_id": f"exp_{index}_{count}",
                "origin": {"ip": "127.0.0.1", "port": 9999},
                "data": {"key": song, "value": str(value)},
            }

            send_request("127.0.0.1", port, msg)
            count += 1

    end = time.time()

    results[index] = {"count": count, "time": end - start}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=5000)
    parser.add_argument("--nodes", type=int, default=10)
    args = parser.parse_args()

    threads = []
    results = {}

    overall_start = time.time()

    for n in range(args.nodes):
        file_path = os.path.join(
            os.path.dirname(__file__), "inserts", f"insert_{n}.txt"
        )

        port = args.base_port + n
        value = n

        t = threading.Thread(target=worker, args=(file_path, port, value, results, n))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    overall_end = time.time()

    # Υπολογισμός system throughput
    total_inserts = sum(results[i]["count"] for i in results)
    max_time = max(results[i]["time"] for i in results)

    system_throughput = total_inserts / max_time

    print("\n===== WRITE EXPERIMENT RESULTS =====")
    print(f"Total inserts: {total_inserts}")
    print(f"Max completion time: {max_time:.4f} sec")
    print(f"System throughput: {system_throughput:.4f} inserts/sec")
    print("====================================\n")


if __name__ == "__main__":
    main()
