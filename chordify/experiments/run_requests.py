import os
import sys
import argparse
import time

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
from net import send_request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=5000)
    parser.add_argument(
        "--node", type=int, default=0, help="Node index that will execute the workload"
    )
    parser.add_argument("--consistency", required=True, choices=["linear", "eventual"])

    args = parser.parse_args()

    node_port = args.base_port + args.node

    requests_file = os.path.join(os.path.dirname(__file__), "requests.txt")

    output_file = os.path.join(
        os.path.dirname(__file__), f"results_{args.consistency}.txt"
    )

    with open(requests_file, "r", encoding="utf-8") as f:
        lines = f.readlines()

    with open(output_file, "w", encoding="utf-8") as out:

        for i, line in enumerate(lines):

            line = line.strip()
            if not line:
                continue

            # ✅ Σωστό parsing με βάση το κόμμα
            parts = [p.strip() for p in line.split(",")]

            operation = parts[0].lower()

            if operation == "insert":
                if len(parts) != 3:
                    continue

                key = parts[1]
                value = parts[2]

                msg = {
                    "type": "INSERT",
                    "req_id": f"mix_insert_{i}",
                    "origin": {"ip": "127.0.0.1", "port": 9999},
                    "data": {"key": key, "value": value},
                }

                send_request("127.0.0.1", node_port, msg)

            elif operation == "query":
                if len(parts) != 2:
                    continue

                key = parts[1]

                msg = {
                    "type": "QUERY",
                    "req_id": f"mix_query_{i}",
                    "origin": {"ip": "127.0.0.1", "port": 9999},
                    "data": {"key": key},
                }

                response = send_request("127.0.0.1", node_port, msg)

                if response and response.get("status") == "OK":
                    result = response["data"].get("result", None)
                else:
                    result = "ERROR"

                out.write(f"{i}: {key} -> {result}\n")

            # μικρό spacing για ordering
            time.sleep(0.05)

    print(f"\nResults saved to {output_file}\n")


if __name__ == "__main__":
    main()
