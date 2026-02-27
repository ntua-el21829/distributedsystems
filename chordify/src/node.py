import argparse
from email.mime import message
from itertools import chain
import uuid
import threading

from hashing import sha1_int, in_interval, in_open_interval, MAX_ID
from storage import Storage
from net import start_server, send_request


class Node:
    def __init__(
        self,
        ip: str,
        port: int,
        k: int = 1,
        consistency: str = "eventual",
        use_fingers: bool = False,
        m: int = 16,
    ):
        self.k = max(1, int(k))
        self.ip = ip
        self.port = port
        self.consistency = consistency

        # Node ID = SHA1(ip:port)
        self.node_id = sha1_int(f"{ip}:{port}")

        # Chord pointers (initially self)
        self.successor = {"ip": ip, "port": port, "id": self.node_id}
        self.predecessor = {"ip": ip, "port": port, "id": self.node_id}

        self.use_fingers = use_fingers
        self.m = m
        self.finger = [dict(self.successor) for _ in range(self.m)]
        self.next_finger_to_fix = 0

        # Local storage (thread-safe)
        self.storage = Storage()

    # --- Ring logic ---

    def is_responsible(self, key_id: int) -> bool:
        # single-node ring: responsible for everything
        if self.predecessor["id"] == self.node_id:
            return True
        return in_interval(key_id, self.predecessor["id"], self.node_id)

    def forward_to_successor(self, message: dict) -> dict:
        succ = self.successor
        return send_request(succ["ip"], succ["port"], message)

    def set_successor(self, node_info: dict) -> None:
        self.successor = node_info

    def set_predecessor(self, node_info: dict) -> None:
        self.predecessor = node_info

    def graceful_depart(self):
        self._do_depart()

    # --- Internal depart logic (business logic) ---
    def _do_depart(self):
        # If single node
        if (
            self.successor["id"] == self.node_id
            and self.predecessor["id"] == self.node_id
        ):
            return

        succ = self.successor
        pred = self.predecessor

        # 1) Transfer all local keys to successor
        all_local = self.storage.get_primary_only()
        items = []
        for key_id, rec in all_local.items():
            items.append(
                {"key_id": int(key_id), "key": rec["key"], "value": rec["value"]}
            )

        if items:
            send_request(
                succ["ip"],
                succ["port"],
                {
                    "type": "BULK_INSERT",
                    "req_id": "depart_transfer",
                    "origin": {"ip": self.ip, "port": self.port},
                    "data": {"items": items},
                },
            )

        # 2) Fix pointers
        send_request(
            pred["ip"],
            pred["port"],
            {
                "type": "SET_SUCCESSOR",
                "req_id": "depart_rewire",
                "origin": {"ip": self.ip, "port": self.port},
                "data": {"node": succ},
            },
        )

        send_request(
            succ["ip"],
            succ["port"],
            {
                "type": "SET_PREDECESSOR",
                "req_id": "depart_rewire",
                "origin": {"ip": self.ip, "port": self.port},
                "data": {"node": pred},
            },
        )

    def get_overlay(self):
        # If single node
        if self.successor["id"] == self.node_id:
            return {
                "status": "OK",
                "data": {
                    "ring": [{"id": self.node_id, "ip": self.ip, "port": self.port}]
                },
            }

        return send_request(
            self.successor["ip"],
            self.successor["port"],
            {
                "type": "OVERLAY",
                "req_id": "overlay",
                "origin": {"ip": self.ip, "port": self.port},
                "data": {
                    "start_id": self.node_id,
                    "acc": [{"id": self.node_id, "ip": self.ip, "port": self.port}],
                },
            },
        )

    def get_ring(self) -> list[dict]:
        # single node
        if (
            self.successor["id"] == self.node_id
            and self.predecessor["id"] == self.node_id
        ):
            return [{"ip": self.ip, "port": self.port, "id": self.node_id}]

        resp = self.handle_message(
            {
                "type": "OVERLAY",
                "req_id": "overlay_for_fingers",
                "origin": {"ip": self.ip, "port": self.port},
                "data": {"start_id": self.node_id, "acc": []},
            }
        )

        if resp.get("status") != "OK":
            return []

        return resp["data"]["ring"]

    def successor_of_id_in_ring(self, ring: list[dict], target_id: int) -> dict:
        if not ring:
            return self.successor

        sorted_ring = sorted(ring, key=lambda n: n["id"])

        for n in sorted_ring:
            if n["id"] >= target_id:
                return {"ip": n["ip"], "port": n["port"], "id": n["id"]}

        # wrap-around
        first = sorted_ring[0]
        return {"ip": first["ip"], "port": first["port"], "id": first["id"]}

    def fix_fingers(self):
        if not self.use_fingers:
            return

        i = self.next_finger_to_fix
        self.next_finger_to_fix = (self.next_finger_to_fix + 1) % self.m

        target_id = (self.node_id + (1 << i)) % MAX_ID

        resp = self.handle_message(
            {
                "type": "FIND_SUCCESSOR",
                "req_id": f"fix_{self.port}_{i}",
                "origin": {"ip": self.ip, "port": self.port},
                "data": {"id": target_id, "ttl": 32},
            }
        )

        if resp.get("status") == "OK":
            self.finger[i] = resp["data"]["successor"]

    def build_finger_table(self) -> None:
        if not self.use_fingers:
            self.finger = []
            return

        ring = self.get_ring()
        if not ring:
            self.finger = []
            return

        new_fingers = []

        for i in range(self.m):
            start = (self.node_id + (1 << i)) % MAX_ID
            succ = self.successor_of_id_in_ring(ring, start)
            new_fingers.append(succ)

        self.finger = new_fingers

    def forward_with_ttl(self, message: dict, target_id: int) -> dict:
        # Clone message (μην κάνεις mutate το original)
        new_msg = dict(message)
        new_msg["data"] = dict(message.get("data", {}))

        ttl = new_msg["data"].get("ttl", 32)

        if ttl <= 0:
            # fallback σε successor
            return send_request(
                self.successor["ip"],
                self.successor["port"],
                new_msg,
            )

        new_msg["data"]["ttl"] = ttl - 1

        # Finger-based επιλογή
        next_node = self.closest_preceding_node(target_id)

        if next_node["id"] == self.node_id:
            next_node = self.successor

        print("FINGER FORWARD from", self.port, "to", next_node["port"], "ttl", ttl)

        return send_request(
            next_node["ip"],
            next_node["port"],
            new_msg,
        )

    def closest_preceding_node(self, target_id: int) -> dict:

        # Guard 1: αν δεν χρησιμοποιούμε fingers
        if not self.use_fingers or not self.finger:
            return self.successor

        # Guard 2: αν ψάχνουμε τον εαυτό μας
        if target_id == self.node_id:
            return self.successor

        # Σάρωση από high -> low (Chord rule)
        for i in range(self.m - 1, -1, -1):
            f = self.finger[i]

            # Robustness guard
            if not f or "id" not in f:
                continue

            fid = f["id"]

            # Μην επιστρέψεις εσένα
            if fid == self.node_id:
                continue

            # Strict open interval (self, target)
            if in_open_interval(fid, self.node_id, target_id):
                return f

        # Fallback
        return self.successor

    # --- Message helpers ---

    def make_msg(
        self, msg_type: str, data: dict | None = None, req_id: str | None = None
    ) -> dict:
        """
        Create a standard envelope message originating from this node.
        """
        return {
            "type": msg_type,
            "req_id": req_id or uuid.uuid4().hex,
            "origin": {"ip": self.ip, "port": self.port},
            "data": data or {},
        }

    def make_response(
        self,
        status: str,
        req_id: str | None = None,
        data: dict | None = None,
        error: str | None = None,
    ) -> dict:
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

    def get_replica_nodes(self) -> list[dict]:
        """
        Return the next (k-1) successors of THIS node in ring order.
        Uses OVERLAY ring-walk (system is stable, no concurrent join/depart).
        """
        if self.k <= 1:
            return []

        # Ask the running ring (through my successor) for overlay starting at me
        if self.successor["id"] == self.node_id:
            return []

        resp = self.handle_message(
            {
                "type": "OVERLAY",
                "req_id": "overlay_for_replicas",
                "origin": {"ip": self.ip, "port": self.port},
                "data": {"start_id": self.node_id, "acc": []},
            }
        )
        if resp.get("status") != "OK":
            return []
        ring = resp["data"]["ring"]
        # find me in ring
        idx = None
        for i, n in enumerate(ring):
            if n["id"] == self.node_id:
                idx = i
                break
        if idx is None:
            return []

        replicas = []
        for step in range(1, min(self.k, len(ring))):
            j = (idx + step) % len(ring)
            replicas.append(
                {"ip": ring[j]["ip"], "port": ring[j]["port"], "id": ring[j]["id"]}
            )
        return replicas

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
            return self.make_response(
                "ERROR", req_id=req_id, error="Missing 'type' field"
            )

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
                    "consistency": self.consistency,
                    "replication_factor": self.k,
                },
            )

        if msg_type == "FINGERS":
            return self.make_response(
                "OK", req_id=req_id, data={"fingers": self.finger}
            )

        # --- Bulk insert (used for key transfers / future replication) ---
        if msg_type == "BULK_INSERT":
            items = data.get("items", [])
            # items: list of {"key_id": int, "key": str, "value": str}
            for it in items:
                self.storage.insert_primary(it["key_id"], it["key"], it["value"])
            return self.make_response("OK", req_id=req_id, data={"count": len(items)})

        # --- Transfer keys to a new node after join ---
        # New node asks its successor to send keys that now belong to new node.
        if msg_type == "TRANSFER_KEYS":
            new_node = data.get("new_node")
            if not new_node:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing data.new_node"
                )

            new_id = new_node["id"]
            # Keys that should move to new node are those in (pred_of_new, new]
            # From the successor's perspective, these are keys in (self.predecessor.id, new_id]
            # because after join, successor.predecessor will be set to new, but timing can vary.
            # We use current predecessor as the left bound.
            left = self.predecessor["id"]
            right = new_id

            all_local = self.storage.get_primary_only()
            to_move = []
            to_delete = []

            for key_id, rec in all_local.items():
                if in_interval(int(key_id), left, right):
                    to_move.append(
                        {
                            "key_id": int(key_id),
                            "key": rec["key"],
                            "value": rec["value"],
                        }
                    )
                    to_delete.append(int(key_id))

            # Send to new node
            if to_move:
                send_request(
                    new_node["ip"],
                    new_node["port"],
                    {
                        "type": "BULK_INSERT",
                        "req_id": req_id,
                        "origin": {"ip": self.ip, "port": self.port},
                        "data": {"items": to_move},
                    },
                )
                # Remove moved keys locally
                for kid in to_delete:
                    self.storage.delete(kid)

            return self.make_response("OK", req_id=req_id, data={"moved": len(to_move)})

            # --- Basic ring queries ---

        if msg_type == "GET_PREDECESSOR":
            return self.make_response(
                "OK", req_id=req_id, data={"predecessor": self.predecessor}
            )

        if msg_type == "GET_SUCCESSOR":
            return self.make_response(
                "OK", req_id=req_id, data={"successor": self.successor}
            )

        if msg_type == "SET_PREDECESSOR":
            node_info = data.get("node")
            if not node_info:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing data.node"
                )
            self.set_predecessor(node_info)
            return self.make_response(
                "OK", req_id=req_id, data={"predecessor": self.predecessor}
            )

        if msg_type == "SET_SUCCESSOR":
            node_info = data.get("node")
            if not node_info:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing data.node"
                )
            self.set_successor(node_info)
            return self.make_response(
                "OK", req_id=req_id, data={"successor": self.successor}
            )

        # --- FIND_SUCCESSOR (Chord routing without finger tables) ---
        # Returns the successor of a target id.
        if msg_type == "FIND_SUCCESSOR":
            target_id = data.get("id")
            if target_id is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing data.id"
                )

            # If only one node in ring
            if (
                self.successor["id"] == self.node_id
                and self.predecessor["id"] == self.node_id
            ):
                return self.make_response(
                    "OK", req_id=req_id, data={"successor": self.successor}
                )

            # If target ∈ (self, successor] -> successor is answer
            if in_interval(target_id, self.node_id, self.successor["id"]):
                return self.make_response(
                    "OK", req_id=req_id, data={"successor": self.successor}
                )

            # Otherwise
            return self.forward_with_ttl(message, target_id=target_id)

        if msg_type == "JOIN_REQUEST":
            new_node = data.get("new_node")
            if not new_node:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing data.new_node"
                )

            alone = (
                self.successor["id"] == self.node_id
                and self.predecessor["id"] == self.node_id
            )
            if alone:
                self.successor = new_node
                self.predecessor = new_node
                return self.make_response(
                    "OK",
                    req_id=req_id,
                    data={
                        "successor": {
                            "ip": self.ip,
                            "port": self.port,
                            "id": self.node_id,
                        },
                        "predecessor": {
                            "ip": self.ip,
                            "port": self.port,
                            "id": self.node_id,
                        },
                        "mode": "two_node_bootstrap",
                    },
                )

            # Normal case: find successor of new node id starting from THIS node
            find_msg = {
                "type": "FIND_SUCCESSOR",
                "req_id": req_id,
                "origin": message.get("origin", {"ip": self.ip, "port": self.port}),
                "data": {"id": new_node["id"]},
            }
            resp = self.handle_message(find_msg)  # may forward via forward_to_successor

            if resp.get("status") != "OK":
                return resp

            return self.make_response(
                "OK",
                req_id=req_id,
                data={"successor": resp["data"]["successor"], "mode": "normal"},
            )
        # --- INSERT ---
        if msg_type == "INSERT":
            key = data.get("key")
            value = data.get("value")

            if key is None or value is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing key or value"
                )

            key_id = sha1_int(key)

            # Linear mode: ensure request reaches PRIMARY first
            if self.consistency == "linear" and not self.is_responsible(key_id):
                if self.use_fingers:
                    return self.forward_with_ttl(message, target_id=key_id)
                else:
                    return self.forward_to_successor(message)

            # If I am responsible (PRIMARY)
            if self.is_responsible(key_id):

                # ================= LINEAR MODE =================
                if self.consistency == "linear":

                    replicas = self.get_replica_nodes()
                    chain = [
                        {"ip": self.ip, "port": self.port, "id": self.node_id}
                    ] + replicas

                    # Apply once at head
                    self.storage.insert_primary(key_id, key, value)

                    record = self.storage.query(key_id)
                    if record is None:
                        return self.make_response(
                            "ERROR",
                            req_id=req_id,
                            error="Failed to store key",
                        )

                    # No replicas (k = 1)
                    if len(chain) == 1:
                        return self.make_response(
                            "OK",
                            req_id=req_id,
                            data={"stored_at": self.port, "mode": "linear_chain"},
                        )

                    # Send to first replica in chain
                    return send_request(
                        chain[1]["ip"],
                        chain[1]["port"],
                        {
                            "type": "CHAIN_PUT",
                            "req_id": req_id,
                            "origin": message.get("origin"),
                            "data": {
                                "key_id": key_id,
                                "key": key,
                                "value": record["value"],
                                "chain": chain,
                                "index": 1,
                            },
                        },
                    )

                # ================= EVENTUAL MODE =================
                self.storage.insert_primary(key_id, key, value)
                record = self.storage.query(key_id)
                if record is None:
                    return self.make_response(
                        "ERROR",
                        req_id=req_id,
                        error="Failed to store key",
                    )
                replicas = self.get_replica_nodes()
                primary_info = {"ip": self.ip, "port": self.port, "id": self.node_id}

                for r in replicas:
                    send_request(
                        r["ip"],
                        r["port"],
                        {
                            "type": "REPLICA_PUT",
                            "req_id": req_id,
                            "origin": {"ip": self.ip, "port": self.port},
                            "data": {
                                "key_id": key_id,
                                "key": key,
                                "value": record["value"],
                                "primary": primary_info,
                            },
                        },
                    )

                return self.make_response(
                    "OK",
                    req_id=req_id,
                    data={"stored_at": self.port, "replicas": len(replicas)},
                )

            # Otherwise forward (eventual fallback)
            return self.forward_to_successor(message)

        # --- QUERY ---
        if msg_type == "QUERY":
            key = data.get("key")
            data.setdefault("path", [])

            if key is None:
                return self.make_response("ERROR", req_id=req_id, error="Missing key")

            # Special case: query "*"
            if key == "*":
                return send_request(
                    self.successor["ip"],
                    self.successor["port"],
                    {
                        "type": "QUERY_ALL",
                        "req_id": req_id,
                        "origin": {"ip": self.ip, "port": self.port},
                        "data": {
                            "start_id": self.node_id,
                            "acc": {f"{self.ip}:{self.port}": self.storage.get_all()},
                        },
                    },
                )

            # -------- Single key --------
            key_id = sha1_int(key)

            # Linear: ensure request reaches PRIMARY first
            if self.consistency == "linear" and not self.is_responsible(key_id):
                data.setdefault("path", []).append(self.port)
                if self.use_fingers:
                    return self.forward_with_ttl(message, target_id=key_id)
                return self.forward_to_successor(message)

            # ================= LINEAR MODE =================
            if self.consistency == "linear" and self.is_responsible(key_id):

                replicas = self.get_replica_nodes()
                chain = [
                    {"ip": self.ip, "port": self.port, "id": self.node_id}
                ] + replicas

                tail = chain[-1]

                # If I am tail (k = 1 or single-node case)
                if tail["id"] == self.node_id:
                    local = self.storage.query(key_id)
                    return self.make_response(
                        "OK",
                        req_id=req_id,
                        data={
                            "result": local,
                            "served_by": self.port,
                            "path": data.get("path", []) + [self.port],
                        },
                    )
                data.setdefault("path", []).append(self.port)
                # Ask tail directly
                return send_request(
                    tail["ip"],
                    tail["port"],
                    {
                        "type": "CHAIN_GET",
                        "req_id": req_id,
                        "origin": message.get("origin"),
                        "data": {"key_id": key_id, "path": data.get("path", [])},
                    },
                )

            # ================= EVENTUAL MODE =================
            local = self.storage.query(key_id)

            if local is not None:
                return self.make_response(
                    "OK",
                    req_id=req_id,
                    data={
                        "result": local,
                        "served_by": self.port,
                        "path": data.get("path", []) + [self.port],
                    },
                )

            if self.is_responsible(key_id):
                return self.make_response(
                    "OK",
                    req_id=req_id,
                    data={
                        "result": None,
                        "served_by": self.port,
                        "path": data.get("path", []) + [self.port],
                    },
                )

            data.setdefault("path", []).append(self.port)
            if self.use_fingers:
                return self.forward_with_ttl(message, target_id=key_id)
            else:
                return self.forward_to_successor(message)

        # --- DELETE ---
        if msg_type == "DELETE":
            key = data.get("key")
            if key is None:
                return self.make_response("ERROR", req_id=req_id, error="Missing key")

            key_id = sha1_int(key)

            # Linear mode: ensure request reaches PRIMARY first
            if self.consistency == "linear" and not self.is_responsible(key_id):
                if self.use_fingers:
                    return self.forward_with_ttl(message, target_id=key_id)
                return self.forward_to_successor(message)

            if self.is_responsible(key_id):
                # Check existence BEFORE deleting
                existing = self.storage.query(key_id)

                if existing is None:
                    return self.make_response(
                        "ERROR", req_id=req_id, error="Key not found"
                    )

                # ================= LINEAR MODE =================
                if self.consistency == "linear":

                    replicas = self.get_replica_nodes()
                    chain = [
                        {"ip": self.ip, "port": self.port, "id": self.node_id}
                    ] + replicas

                    # delete at head
                    self.storage.delete(key_id)

                    # no replicas (k = 1)
                    if len(chain) == 1:
                        return self.make_response(
                            "OK", req_id=req_id, data={"deleted_from": self.port}
                        )

                    # send to first replica in chain
                    return send_request(
                        chain[1]["ip"],
                        chain[1]["port"],
                        {
                            "type": "CHAIN_DELETE",
                            "req_id": req_id,
                            "origin": message.get("origin"),
                            "data": {"key_id": key_id, "chain": chain, "index": 1},
                        },
                    )

                # ================= EVENTUAL MODE =================
                self.storage.delete(key_id)

                replicas = self.get_replica_nodes()
                for r in replicas:
                    send_request(
                        r["ip"],
                        r["port"],
                        {
                            "type": "REPLICA_DELETE",
                            "req_id": req_id,
                            "origin": {"ip": self.ip, "port": self.port},
                            "data": {"key_id": key_id},
                        },
                    )

                return self.make_response(
                    "OK",
                    req_id=req_id,
                    data={"deleted_from": self.port, "replicas": len(replicas)},
                )

            if self.use_fingers:
                return self.forward_with_ttl(message, target_id=key_id)
            else:
                return self.forward_to_successor(message)

            # --- QUERY ALL ("*") ---
        if msg_type == "QUERY_ALL":
            start_id = data.get("start_id")
            acc = data.get("acc", {})

            # Add my local data
            acc[f"{self.ip}:{self.port}"] = self.storage.get_all()

            # If successor is the originator, stop
            if self.successor["id"] == start_id:
                return self.make_response("OK", req_id=req_id, data={"result": acc})

            # Otherwise forward
            return send_request(
                self.successor["ip"],
                self.successor["port"],
                {
                    "type": "QUERY_ALL",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {"start_id": start_id, "acc": acc},
                },
            )

        # --- DEPART (graceful) ---
        if msg_type == "DEPART":
            self._do_depart()
            return self.make_response(
                "OK", req_id=req_id, data={"msg": "Depart completed"}
            )

        if msg_type == "REBUILD_FINGERS_RING":
            start_id = data.get("start_id")

            if self.use_fingers:
                self.build_finger_table()

            if self.successor["id"] == start_id:
                return self.make_response("OK", req_id=req_id)

            return send_request(
                self.successor["ip"],
                self.successor["port"],
                {
                    "type": "REBUILD_FINGERS_RING",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {"start_id": start_id},
                },
            )

        # --- OVERLAY ---
        if msg_type == "OVERLAY":
            start_id = data.get("start_id")
            acc = data.get("acc", [])

            # first call from client
            if start_id is None:
                start_id = self.node_id
                acc = []

            # add myself
            acc.append({"id": self.node_id, "ip": self.ip, "port": self.port})

            # stop condition
            if self.successor["id"] == start_id:
                return self.make_response("OK", req_id=req_id, data={"ring": acc})

            # forward
            return send_request(
                self.successor["ip"],
                self.successor["port"],
                {
                    "type": "OVERLAY",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {"start_id": start_id, "acc": acc},
                },
            )

        if msg_type == "CHAIN_PUT":

            kid = data.get("key_id")
            key = data.get("key")
            value = data.get("value")
            chain = data.get("chain")
            idx = data.get("index")

            if (
                kid is None
                or key is None
                or value is None
                or chain is None
                or idx is None
            ):
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing fields in CHAIN_PUT"
                )

            key_id = int(kid)
            index = int(idx)

            primary_info = chain[0]
            self.storage.put_replica(key_id, key, value, primary_info)

            if index == len(chain) - 1:
                return self.make_response(
                    "OK", req_id=req_id, data={"committed_at": self.port}
                )

            next_node = chain[index + 1]

            return send_request(
                next_node["ip"],
                next_node["port"],
                {
                    "type": "CHAIN_PUT",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {
                        "key_id": key_id,
                        "key": key,
                        "value": value,
                        "chain": chain,
                        "index": index + 1,
                    },
                },
            )

        if msg_type == "CHAIN_GET":

            kid = data.get("key_id")
            if kid is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing key_id"
                )

            key_id = int(kid)

            #  Πάρε το path που ήρθε
            path = data.get("path", [])

            #  Πρόσθεσε το current node (tail)
            path = path + [self.port]

            local = self.storage.query(key_id)

            return self.make_response(
                "OK",
                req_id=req_id,
                data={
                    "result": local,
                    "served_by": self.port,
                    "path": path,
                },
            )

        if msg_type == "CHAIN_DELETE":

            kid = data.get("key_id")
            chain = data.get("chain")
            idx = data.get("index")

            if kid is None or chain is None or idx is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing key_id"
                )

            key_id = int(kid)
            index = int(idx)

            self.storage.delete(key_id)

            if index == len(chain) - 1:
                return self.make_response(
                    "OK", req_id=req_id, data={"deleted_at": self.port}
                )

            next_node = chain[index + 1]

            return send_request(
                next_node["ip"],
                next_node["port"],
                {
                    "type": "CHAIN_DELETE",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {"key_id": key_id, "chain": chain, "index": index + 1},
                },
            )

        # --- REPLICA_PUT (store replica copy) ---
        if msg_type == "REPLICA_PUT":
            key_id = data.get("key_id")
            key = data.get("key")
            value = data.get("value")
            primary = data.get("primary")
            if key_id is None or key is None or value is None or primary is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing replica fields"
                )

            self.storage.put_replica(int(key_id), key, value, primary)
            return self.make_response(
                "OK", req_id=req_id, data={"replica_at": self.port}
            )

        # --- REPLICA_DELETE (delete replica copy) ---
        if msg_type == "REPLICA_DELETE":
            key_id = data.get("key_id")
            if key_id is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing key_id"
                )

            self.storage.delete(int(key_id))
            return self.make_response(
                "OK", req_id=req_id, data={"replica_deleted_at": self.port}
            )

        # --- REPAIR_REPLICAS: re-push replicas for all PRIMARY keys ---
        if msg_type == "REPAIR_REPLICAS":
            primary_items = self.storage.get_primary_only()
            replicas = self.get_replica_nodes()
            primary_info = {"ip": self.ip, "port": self.port, "id": self.node_id}

            pushed = 0
            for key_id, rec in primary_items.items():
                for r in replicas:
                    send_request(
                        r["ip"],
                        r["port"],
                        {
                            "type": "REPLICA_PUT",
                            "req_id": req_id,
                            "origin": {"ip": self.ip, "port": self.port},
                            "data": {
                                "key_id": int(key_id),
                                "key": rec["key"],
                                "value": rec["value"],
                                "primary": primary_info,
                            },
                        },
                    )
                    pushed += 1

            return self.make_response("OK", req_id=req_id, data={"pushed": pushed})

        # --- CLEAR_REPLICAS: delete all replica entries locally ---
        if msg_type == "CLEAR_REPLICAS":
            self.storage.delete_replicas_only()
            return self.make_response(
                "OK", req_id=req_id, data={"msg": "Replicas cleared"}
            )

        # --- CLEAR_REPLICAS_RING: walk ring and clear replicas on every node ---
        if msg_type == "CLEAR_REPLICAS_RING":
            start_id = data.get("start_id")
            if start_id is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing start_id"
                )

            # clear myself
            self.storage.delete_replicas_only()

            # stop condition
            if self.successor["id"] == start_id:
                return self.make_response(
                    "OK", req_id=req_id, data={"msg": "Clear replicas completed"}
                )

            return send_request(
                self.successor["ip"],
                self.successor["port"],
                {
                    "type": "CLEAR_REPLICAS_RING",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {"start_id": start_id},
                },
            )

        # --- REPAIR_RING: walk ring and call REPAIR_REPLICAS on every node ---
        if msg_type == "REPAIR_RING":
            start_id = data.get("start_id")
            if start_id is None:
                return self.make_response(
                    "ERROR", req_id=req_id, error="Missing start_id"
                )

            # repair myself
            primary_items = self.storage.get_primary_only()
            replicas = self.get_replica_nodes()
            primary_info = {"ip": self.ip, "port": self.port, "id": self.node_id}

            pushed = 0
            for key_id, rec in primary_items.items():
                for r in replicas:
                    send_request(
                        r["ip"],
                        r["port"],
                        {
                            "type": "REPLICA_PUT",
                            "req_id": req_id,
                            "origin": {"ip": self.ip, "port": self.port},
                            "data": {
                                "key_id": int(key_id),
                                "key": rec["key"],
                                "value": rec["value"],
                                "primary": primary_info,
                            },
                        },
                    )
                    pushed += 1

            # stop condition
            if self.successor["id"] == start_id:
                return self.make_response(
                    "OK", req_id=req_id, data={"msg": "Repair completed"}
                )

            return send_request(
                self.successor["ip"],
                self.successor["port"],
                {
                    "type": "REPAIR_RING",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {"start_id": start_id},
                },
            )

        return self.make_response(
            "UNKNOWN", req_id=req_id, data={"received_type": msg_type}
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)

    parser.add_argument(
        "--bootstrap", action="store_true", help="Run as bootstrap node"
    )
    parser.add_argument("--bootstrap-ip", default="127.0.0.1")
    parser.add_argument("--bootstrap-port", type=int, default=None)

    parser.add_argument("--overlay", action="store_true", help="Print ring topology")
    parser.add_argument(
        "--depart", action="store_true", help="Gracefully leave the ring"
    )

    parser.add_argument(
        "--k",
        type=int,
        default=1,
        help="Replication factor (primary + k-1 successors)",
    )
    parser.add_argument(
        "--consistency", choices=["eventual", "linear"], default="eventual"
    )

    parser.add_argument(
        "--repair-after-join",
        action="store_true",
        help="After successful join, ask bootstrap to rebuild replicas (REPAIR_RING)",
    )
    parser.add_argument("--use-fingers", action="store_true")
    parser.add_argument("--m", type=int, default=16)

    args = parser.parse_args()

    node = Node(
        args.ip,
        args.port,
        k=args.k,
        consistency=args.consistency,
        use_fingers=args.use_fingers,
        m=args.m,
    )

    def get_node_id(ip: str, port: int) -> int:
        ping = send_request(
            ip,
            port,
            {
                "type": "PING",
                "req_id": f"ping_{port}",
                "origin": {"ip": args.ip, "port": args.port},
                "data": {},
            },
        )
        if ping.get("status") != "OK":
            raise RuntimeError(f"PING failed for {ip}:{port}: {ping}")
        return ping["data"]["node_id"]

    def maybe_repair_ring(tag: str) -> None:
        # Needs bootstrap info; if missing, do nothing
        if args.bootstrap_port is None:
            return

        try:
            bs_id = get_node_id(args.bootstrap_ip, args.bootstrap_port)

            # 1) CLEAR stale replicas ring-wide (critical to keep exactly k copies)
            clr = send_request(
                args.bootstrap_ip,
                args.bootstrap_port,
                {
                    "type": "CLEAR_REPLICAS_RING",
                    "req_id": f"clear_{tag}_{args.port}",
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {"start_id": bs_id},
                },
            )
            print("CLEAR_REPLICAS_RING:", clr)

            # 2) Rebuild replicas ring-wide
            rep = send_request(
                args.bootstrap_ip,
                args.bootstrap_port,
                {
                    "type": "REPAIR_RING",
                    "req_id": f"repair_{tag}_{args.port}",
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {"start_id": bs_id},
                },
            )
            print("REPAIR_RING:", rep)

            # 3) Optional: rebuild fingers ring-wide
            if args.use_fingers:
                rf = send_request(
                    args.bootstrap_ip,
                    args.bootstrap_port,
                    {
                        "type": "REBUILD_FINGERS_RING",
                        "req_id": f"rebuild_fingers_{tag}_{args.port}",
                        "origin": {"ip": args.ip, "port": args.port},
                        "data": {"start_id": bs_id},
                    },
                )
                print("REBUILD_FINGERS_RING:", rf)

        except Exception as e:
            print("Ring maintenance failed:", e)

    # ---- overlay CLI ----
    if args.overlay:
        result = send_request(
            args.ip,
            args.port,
            {
                "type": "OVERLAY",
                "req_id": "overlay_cli",
                "origin": {"ip": args.ip, "port": args.port},
                "data": {"start_id": node.node_id, "acc": []},
            },
        )
        print(result)
        raise SystemExit(0)

    # ---- depart CLI ----
    if args.depart:
        result = send_request(
            args.ip,
            args.port,
            {
                "type": "DEPART",
                "req_id": "depart_cli",
                "origin": {"ip": args.ip, "port": args.port},
                "data": {},
            },
        )
        print(result)

        # After a topology change, rebuild replicas if possible
        if args.k > 1:
            maybe_repair_ring("after_depart")

        if args.use_fingers and args.bootstrap_port is not None:
            bs_id = get_node_id(args.bootstrap_ip, args.bootstrap_port)

            send_request(
                args.bootstrap_ip,
                args.bootstrap_port,
                {
                    "type": "REBUILD_FINGERS_RING",
                    "req_id": f"rebuild_fingers_after_depart_{args.port}",
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {"start_id": bs_id},
                },
            )

        raise SystemExit(0)

    # Start server in background
    server_thread = threading.Thread(
        target=start_server, args=(node, args.ip, args.port), daemon=True
    )
    server_thread.start()

    # -------------------------
    # JOIN LOGIC (non-bootstrap)
    # -------------------------
    if not args.bootstrap:
        if args.bootstrap_port is None:
            print("ERROR: Non-bootstrap node needs --bootstrap-port")
            raise SystemExit(1)

        join_resp = send_request(
            args.bootstrap_ip,
            args.bootstrap_port,
            {
                "type": "JOIN_REQUEST",
                "req_id": "join_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "new_node": {"ip": node.ip, "port": node.port, "id": node.node_id}
                },
            },
        )

        if join_resp.get("status") != "OK":
            print("JOIN failed:", join_resp)
            raise SystemExit(1)

        mode = join_resp["data"].get("mode")

        # 2-node ring bootstrap shortcut
        if mode == "two_node_bootstrap":
            node.successor = join_resp["data"]["successor"]
            node.predecessor = join_resp["data"]["predecessor"]

            send_request(
                args.bootstrap_ip,
                args.bootstrap_port,
                {
                    "type": "SET_SUCCESSOR",
                    "req_id": "set_succ_" + str(args.port),
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {
                        "node": {"ip": node.ip, "port": node.port, "id": node.node_id}
                    },
                },
            )

            send_request(
                args.bootstrap_ip,
                args.bootstrap_port,
                {
                    "type": "SET_PREDECESSOR",
                    "req_id": "set_pred_" + str(args.port),
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {
                        "node": {"ip": node.ip, "port": node.port, "id": node.node_id}
                    },
                },
            )

            send_request(
                node.successor["ip"],
                node.successor["port"],
                {
                    "type": "TRANSFER_KEYS",
                    "req_id": "xfer_" + str(args.port),
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {
                        "new_node": {
                            "ip": node.ip,
                            "port": node.port,
                            "id": node.node_id,
                        }
                    },
                },
            )

            print(
                f"Joined ring (2-node). My pred={node.predecessor} succ={node.successor}"
            )

        # normal join
        else:
            succ = join_resp["data"]["successor"]
            node.successor = succ

            pred_resp = send_request(
                succ["ip"],
                succ["port"],
                {
                    "type": "GET_PREDECESSOR",
                    "req_id": "get_pred_" + str(args.port),
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {},
                },
            )
            if pred_resp.get("status") != "OK":
                print("GET_PREDECESSOR failed:", pred_resp)
                raise SystemExit(1)

            pred = pred_resp["data"]["predecessor"]
            node.predecessor = pred

            send_request(
                succ["ip"],
                succ["port"],
                {
                    "type": "SET_PREDECESSOR",
                    "req_id": "set_pred_succ_" + str(args.port),
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {
                        "node": {"ip": node.ip, "port": node.port, "id": node.node_id}
                    },
                },
            )

            send_request(
                pred["ip"],
                pred["port"],
                {
                    "type": "SET_SUCCESSOR",
                    "req_id": "set_succ_pred_" + str(args.port),
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {
                        "node": {"ip": node.ip, "port": node.port, "id": node.node_id}
                    },
                },
            )

            send_request(
                succ["ip"],
                succ["port"],
                {
                    "type": "TRANSFER_KEYS",
                    "req_id": "xfer_" + str(args.port),
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {
                        "new_node": {
                            "ip": node.ip,
                            "port": node.port,
                            "id": node.node_id,
                        }
                    },
                },
            )

            print(f"Joined ring. My pred={node.predecessor} succ={node.successor}")

        # After join, rebuild replicas if requested
        if args.k > 1:
            maybe_repair_ring("after_join")

        if args.use_fingers:
            bs_id = get_node_id(args.bootstrap_ip, args.bootstrap_port)
            send_request(
                args.bootstrap_ip,
                args.bootstrap_port,
                {
                    "type": "REBUILD_FINGERS_RING",
                    "req_id": f"rebuild_fingers_after_join_{args.port}",
                    "origin": {"ip": args.ip, "port": args.port},
                    "data": {"start_id": bs_id},
                },
            )

    # Keep process alive (server thread runs forever)
    threading.Event().wait()
