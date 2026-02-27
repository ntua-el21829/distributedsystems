import argparse
from email import message
from turtle import title
from unittest import result
from typing import Dict, Any, Optional, List
import uuid
import sys

from net import send_request


class ChordifyClient:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port

    def _make_request(self, msg_type: str, data: dict):
        req_id = str(uuid.uuid4())

        message = {
            "type": msg_type,
            "req_id": req_id,
            "origin": {"ip": "client", "port": 0},
            "data": data,
        }

        try:
            response = send_request(self.ip, self.port, message)
            return response
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}

    # ---------------- COMMANDS ---------------- #
    def insert(self, key: str, value: str):
        resp: Dict[str, Any] = self._make_request(
            "INSERT", {"key": key, "value": value}
        )

        if resp.get("status") != "OK":
            self._print_error(resp.get("error", "Insert failed"))
            return

        data: Dict[str, Any] = resp.get("data") or {}

        if "committed_at" in data:
            self._print_ok(
                f"Write committed at node {data['committed_at']} (Linear Chain)"
            )
        elif "stored_at" in data:
            replicas = data.get("replicas", 0)
            self._print_ok(
                f"Stored at node {data['stored_at']} with {replicas} replica(s)"
            )
        else:
            self._print_ok("Insert completed.")

    def delete(self, key: str):
        resp: Dict[str, Any] = self._make_request("DELETE", {"key": key})

        if resp.get("status") != "OK":
            self._print_error(resp.get("error", "Delete failed"))
            return

        data: Dict[str, Any] = resp.get("data") or {}
        deleted_at = data.get("deleted_at")

        if deleted_at is not None:
            self._print_ok(f"Deleted at node {deleted_at}")
        else:
            self._print_ok("Delete completed.")

    def query(self, key: str):
        resp: Dict[str, Any] = self._make_request("QUERY", {"key": key})

        if resp.get("status") != "OK":
            self._print_error(resp.get("error", "Query failed"))
            return

        data: Dict[str, Any] = resp.get("data") or {}
        result: Optional[Dict[str, Any]] = data.get("result")

        # ---------------- QUERY * ----------------
        if key == "*":
            self._print_header("FULL DHT DUMP")

            if not result:
                self._print_ok("No data stored in DHT.")
                return

            for node, items in result.items():
                self._print_header(f"Node {node}")

                if not items:
                    print("  (empty)")
                    continue

                for _, entry in items.items():
                    print(f"  {entry.get('key')}  →  {entry.get('value')}")

            print()
            return

        # ---------------- SINGLE KEY ----------------
        if result is None:
            self._print_ok("Key not found.")
            return

        self._print_header("QUERY RESULT")

        print(f"Value      : {result.get('value')}")
        print(f"Served by  : {data.get('served_by')}")

        role = "REPLICA" if result.get("is_replica") else "PRIMARY"
        print(f"Role       : {role}")

        path = data.get("path")
        if path:
            print(f"Path       : {' → '.join(str(p) for p in path)}")
        else:
            print("Path       : No path available")

        print()

    def overlay(self) -> None:
        resp: Dict[str, Any] = self._make_request(
            "OVERLAY", {"start_id": None, "acc": []}
        )

        if resp.get("status") != "OK":
            self._print_error(str(resp.get("error", "Overlay failed")))
            return

        data: Dict[str, Any] = resp.get("data") or {}
        ring: List[Dict[str, Any]] = data.get("ring") or []

        self._print_header("RING TOPOLOGY")

        if not ring:
            self._print_ok("No nodes in ring.")
            return

        for node in ring:
            if isinstance(node, dict):
                port = node.get("port", "?")
                succ = node.get("successor", {}).get("port", "?")
                pred = node.get("predecessor", {}).get("port", "?")

                print(f"Node {port}")

        print()

    def depart(self):
        resp = self._make_request("DEPART", {})
        print(resp)

    def _print_ok(self, message):
        print(f"\n✔ {message}\n")

    def _print_error(self, message):
        print(f"\n✖ ERROR: {message}\n")

    def _print_header(self, title):
        print("\n" + "=" * 50)
        print(title)
        print("=" * 50)

    def help(self):
        print("\nAvailable commands:")
        print("  insert <key> <value>   Insert a key-value pair")
        print("  delete <key>           Delete a key")
        print("  query <key>            Query a key (or '*' for all)")
        print("  overlay                Print ring topology")
        print("  depart                 Gracefully depart node")
        print("  help                   Show this message")
        print("  exit                   Exit client\n")

    # --------------- INTERACTIVE LOOP --------------- #

    def run(self):
        print("Chordify CLI connected to", f"{self.ip}:{self.port}")
        print("Type 'help' for commands.\n")

        while True:
            try:
                command = input("> ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\nExiting.")
                break

            if not command:
                continue

            parts = command.split()

            cmd = parts[0].lower()

            try:
                if cmd == "insert":
                    if len(parts) < 3:
                        print("Usage: insert <key> <value>")
                        continue
                    key = parts[1]
                    value = " ".join(parts[2:])
                    self.insert(key, value)

                elif cmd == "delete":
                    if len(parts) != 2:
                        print("Usage: delete <key>")
                        continue
                    self.delete(parts[1])

                elif cmd == "query":
                    if len(parts) != 2:
                        print("Usage: query <key>")
                        continue
                    self.query(parts[1])

                elif cmd == "overlay":
                    self.overlay()

                elif cmd == "depart":
                    self.depart()

                elif cmd == "help":
                    self.help()

                elif cmd == "exit":
                    print("Bye.")
                    break

                else:
                    print("Unknown command. Type 'help'.")

            except Exception as e:
                print("Error:", e)


def main():
    parser = argparse.ArgumentParser(description="Chordify CLI Client")
    parser.add_argument("--ip", required=True, help="Node IP to connect to")
    parser.add_argument("--port", required=True, type=int, help="Node port")

    args = parser.parse_args()

    client = ChordifyClient(args.ip, args.port)
    client.run()


if __name__ == "__main__":
    main()
