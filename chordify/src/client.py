import argparse
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
        resp = self._make_request("INSERT", {"key": key, "value": value})
        print(resp)

    def delete(self, key: str):
        resp = self._make_request("DELETE", {"key": key})
        print(resp)

    def query(self, key: str):
        resp = self._make_request("QUERY", {"key": key})
        print(resp)

    def overlay(self):
        resp = self._make_request("OVERLAY", {})
        print(resp)

    def depart(self):
        resp = self._make_request("DEPART", {})
        print(resp)

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


# -------------------- MAIN -------------------- #

def main():
    parser = argparse.ArgumentParser(description="Chordify CLI Client")
    parser.add_argument("--ip", required=True, help="Node IP to connect to")
    parser.add_argument("--port", required=True, type=int, help="Node port")

    args = parser.parse_args()

    client = ChordifyClient(args.ip, args.port)
    client.run()


if __name__ == "__main__":
    main()