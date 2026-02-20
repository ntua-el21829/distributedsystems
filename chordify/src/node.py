import argparse
import uuid

from hashing import sha1_int, in_interval
from storage import Storage
from net import start_server


class Node:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port

        # Node ID = SHA1(ip:port)
        self.node_id = sha1_int(f"{ip}:{port}")

        # Chord pointers (initially self)
        self.successor = {"ip": ip, "port": port, "id": self.node_id}
        self.predecessor = {"ip": ip, "port": port, "id": self.node_id}

        # Local storage (thread-safe)
        self.storage = Storage()

    # --- Message helpers ---

    def make_msg(self, msg_type: str, data: dict | None = None, req_id: str | None = None) -> dict:
        """
        Create a standard envelope message originating from this node.
        """
        return {
            "type": msg_type,
            "req_id": req_id or uuid.uuid4().hex,
            "origin": {"ip": self.ip, "port": self.port},
            "data": data or {},
        }

    def make_response(self, status: str, req_id: str | None = None, data: dict | None = None, error: str | None = None) -> dict:
        """
        Create a standard envelope response.
        """
        resp = {
            "status": status,
            "req_id": req_id,
            "data": data or {},
        }
        if error is not None:
            resp["error"] = error
        return resp

    # --- Core request handler ---

    def handle_message(self, message: dict) -> dict:
        """
        Handle incoming messages (already parsed from JSON).
        Expected message format:
        {
          "type": "...",
          "req_id": "...",
          "origin": {"ip": "...", "port": ...},
          "data": {...}
        }
        """
        msg_type = message.get("type")
        req_id = message.get("req_id")
        data = message.get("data", {})

        # Minimal validation
        if not msg_type:
            return self.make_response("ERROR", req_id=req_id, error="Missing 'type' field")

        if msg_type == "PING":
            return self.make_response(
                "OK",
                req_id=req_id,
                data={
                    "node_id": self.node_id,
                    "ip": self.ip,
                    "port": self.port,
                    "successor": self.successor,
                    "predecessor": self.predecessor,
                },
            )

        return self.make_response("UNKNOWN", req_id=req_id, data={"received_type": msg_type})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)

    args = parser.parse_args()

    node = Node(args.ip, args.port)
    start_server(node, args.ip, args.port)
