import argparse
from email import message
from email.mime import message
import uuid

from hashing import sha1_int, in_interval
from storage import Storage
from net import start_server, send_request


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
    
   
   # --- Ring logic ---

    def is_responsible(self, key_id: int) -> bool:
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
       if self.successor["id"] == self.node_id and self.predecessor["id"] == self.node_id:
           return

       succ = self.successor
       pred = self.predecessor

       # 1) Transfer all local keys to successor
       all_local = self.storage.get_all()
       items = []
       for key_id, rec in all_local.items():
           items.append({
               "key_id": int(key_id),
               "key": rec["key"],
               "value": rec["value"]
           })

       if items:
           send_request(
               succ["ip"],
               succ["port"],
               {
                   "type": "BULK_INSERT",
                   "req_id": "depart_transfer",
                   "origin": {"ip": self.ip, "port": self.port},
                   "data": {"items": items}
               }
           )

       # 2) Fix pointers
       send_request(
           pred["ip"],
           pred["port"],
           {
               "type": "SET_SUCCESSOR",
               "req_id": "depart_rewire",
               "origin": {"ip": self.ip, "port": self.port},
               "data": {"node": succ}
           }
       )

       send_request(
           succ["ip"],
           succ["port"],
           {
               "type": "SET_PREDECESSOR",
               "req_id": "depart_rewire",
               "origin": {"ip": self.ip, "port": self.port},
               "data": {"node": pred}
           }
       )

    def get_overlay(self):
        # If single node
        if self.successor["id"] == self.node_id:
            return {
                "status": "OK",
                "data": {
                    "ring": [{
                        "id": self.node_id,
                        "ip": self.ip,
                        "port": self.port
                    }]
                }
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
                    "acc": [{
                        "id": self.node_id,
                        "ip": self.ip,
                        "port": self.port
                    }]
                }
            }
        )

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
            
        # --- Bulk insert (used for key transfers / future replication) ---
        if msg_type == "BULK_INSERT":
           items = data.get("items", [])
           # items: list of {"key_id": int, "key": str, "value": str}
           for it in items:
               self.storage.insert(it["key_id"], it["key"], it["value"])
           return self.make_response("OK", req_id=req_id, data={"count": len(items)})

       # --- Transfer keys to a new node after join ---
       # New node asks its successor to send keys that now belong to new node.
        if msg_type == "TRANSFER_KEYS":
           new_node = data.get("new_node")
           if not new_node:
               return self.make_response("ERROR", req_id=req_id, error="Missing data.new_node")

           new_id = new_node["id"]
           # Keys that should move to new node are those in (pred_of_new, new]
           # From the successor's perspective, these are keys in (self.predecessor.id, new_id]
           # because after join, successor.predecessor will be set to new, but timing can vary.
           # We use current predecessor as the left bound.
           left = self.predecessor["id"]
           right = new_id

           all_local = self.storage.get_all()
           to_move = []
           to_delete = []

           for key_id, rec in all_local.items():
               if in_interval(int(key_id), left, right):
                   to_move.append({"key_id": int(key_id), "key": rec["key"], "value": rec["value"]})
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
           return self.make_response("OK", req_id=req_id, data={"predecessor": self.predecessor})

        if msg_type == "GET_SUCCESSOR":
           return self.make_response("OK", req_id=req_id, data={"successor": self.successor})

        if msg_type == "SET_PREDECESSOR":
           node_info = data.get("node")
           if not node_info:
               return self.make_response("ERROR", req_id=req_id, error="Missing data.node")
           self.set_predecessor(node_info)
           return self.make_response("OK", req_id=req_id, data={"predecessor": self.predecessor})

        if msg_type == "SET_SUCCESSOR":
           node_info = data.get("node")
           if not node_info:
               return self.make_response("ERROR", req_id=req_id, error="Missing data.node")
           self.set_successor(node_info)
           return self.make_response("OK", req_id=req_id, data={"successor": self.successor})

       # --- FIND_SUCCESSOR (Chord routing without finger tables) ---
       # Returns the successor of a target id.
        if msg_type == "FIND_SUCCESSOR":
           target_id = data.get("id")
           if target_id is None:
               return self.make_response("ERROR", req_id=req_id, error="Missing data.id")

           # If only one node in ring
           if self.successor["id"] == self.node_id and self.predecessor["id"] == self.node_id:
               return self.make_response("OK", req_id=req_id, data={"successor": self.successor})

           # If target ∈ (self, successor] -> successor is answer
           if in_interval(target_id, self.node_id, self.successor["id"]):
               return self.make_response("OK", req_id=req_id, data={"successor": self.successor})

           # Otherwise forward to successor
           return self.forward_to_successor(message)

        if msg_type == "JOIN_REQUEST":
            new_node = data.get("new_node")
            if not new_node:
                return self.make_response("ERROR", req_id=req_id, error="Missing data.new_node")

            alone = (self.successor["id"] == self.node_id and self.predecessor["id"] == self.node_id)
            if alone:
                self.successor = new_node
                self.predecessor = new_node
                return self.make_response(
                    "OK",
                    req_id=req_id,
                    data={
                        "successor": {"ip": self.ip, "port": self.port, "id": self.node_id},
                        "predecessor": {"ip": self.ip, "port": self.port, "id": self.node_id},
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
                return self.make_response("ERROR", req_id=req_id, error="Missing key or value")

            key_id = sha1_int(key)

            # If I am responsible -> store locally
            if self.is_responsible(key_id):
                self.storage.insert(key_id, key, value)
                return self.make_response("OK", req_id=req_id, data={"stored_at": self.port})

            # Otherwise forward
            return self.forward_to_successor(message)
        
                # --- QUERY single key ---
        if msg_type == "QUERY":
            key = data.get("key")
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
                            "acc": {f"{self.ip}:{self.port}": self.storage.get_all()}
                        }
                    }
                )

            key_id = sha1_int(key)

            if self.is_responsible(key_id):
                result = self.storage.query(key_id)
                return self.make_response("OK", req_id=req_id, data={"result": result})

            return self.forward_to_successor(message)
        
                # --- DELETE ---
        if msg_type == "DELETE":
            key = data.get("key")
            if key is None:
                return self.make_response("ERROR", req_id=req_id, error="Missing key")

            key_id = sha1_int(key)

            if self.is_responsible(key_id):
                self.storage.delete(key_id)
                return self.make_response("OK", req_id=req_id, data={"deleted_from": self.port})

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
                    "data": {
                        "start_id": start_id,
                        "acc": acc
                    }
                }
            )
        
          # --- DEPART (graceful) ---
        if msg_type == "DEPART":
           self._do_depart()
           return self.make_response("OK", req_id=req_id, data={"msg": "Depart completed"})

                # --- OVERLAY ---
        # --- OVERLAY ---
        if msg_type == "OVERLAY":
            start_id = data.get("start_id")
            acc = data.get("acc", [])

            # first call from client
            if start_id is None:
                start_id = self.node_id
                acc = []

            # add myself
            acc.append({
                "id": self.node_id,
                "ip": self.ip,
                "port": self.port
            })

            # stop condition
            if self.successor["id"] == start_id:
                return self.make_response(
                    "OK",
                    req_id=req_id,
                    data={"ring": acc}
                )

            # forward
            return send_request(
                self.successor["ip"],
                self.successor["port"],
                {
                    "type": "OVERLAY",
                    "req_id": req_id,
                    "origin": message.get("origin"),
                    "data": {
                        "start_id": start_id,
                        "acc": acc
                    },
                },
            )

        return self.make_response("UNKNOWN", req_id=req_id, data={"received_type": msg_type})


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)

    parser.add_argument("--bootstrap", action="store_true",
                        help="Run as bootstrap node")
    parser.add_argument("--bootstrap-ip", default="127.0.0.1")
    parser.add_argument("--bootstrap-port", type=int, default=None)
    parser.add_argument("--overlay", action="store_true", help="Print ring topology")
    parser.add_argument("--depart", action="store_true", help="Gracefully leave the ring")

    args = parser.parse_args()

    node = Node(args.ip, args.port)
    if args.overlay:
        result = send_request(
            args.ip,
            args.port,
            {
                "type": "OVERLAY",
                "req_id": "overlay_cli",
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "start_id": node.node_id,
                "acc": []
                }
            }
        )
        print(result)
        raise SystemExit(0)

    if args.depart:
        result = send_request(
            args.ip,
            args.port,
            {
                "type": "DEPART",
                "req_id": "depart_cli",
                "origin": {"ip": args.ip, "port": args.port},
                "data": {}
            }
        )
        print(result)
        raise SystemExit(0)
    # -------------------------
    # JOIN LOGIC (non-bootstrap)
    # -------------------------
    if not args.bootstrap:
        if args.bootstrap_port is None:
            print("ERROR: Non-bootstrap node needs --bootstrap-port")
            raise SystemExit(1)

        # 1) Send JOIN_REQUEST to bootstrap
        join_resp = send_request(
            args.bootstrap_ip,
            args.bootstrap_port,
            {
                "type": "JOIN_REQUEST",
                "req_id": "join_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "new_node": {
                        "ip": node.ip,
                        "port": node.port,
                        "id": node.node_id
                    }
                },
            },
        )

        if join_resp.get("status") != "OK":
            print("JOIN failed:", join_resp)
            raise SystemExit(1)

        mode = join_resp["data"].get("mode")

        # -------------------------
        # 2-NODE RING CASE
        # -------------------------
        if mode == "two_node_bootstrap":
            node.successor = join_resp["data"]["successor"]
            node.predecessor = join_resp["data"]["predecessor"]

            # Tell bootstrap to update pointers to me
            send_request(args.bootstrap_ip, args.bootstrap_port, {
                "type": "SET_SUCCESSOR",
                "req_id": "set_succ_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "node": {
                        "ip": node.ip,
                        "port": node.port,
                        "id": node.node_id
                    }
                },
            })

            send_request(args.bootstrap_ip, args.bootstrap_port, {
                "type": "SET_PREDECESSOR",
                "req_id": "set_pred_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "node": {
                        "ip": node.ip,
                        "port": node.port,
                        "id": node.node_id
                    }
                },
            })

            # Ask successor (bootstrap) to transfer keys
            send_request(node.successor["ip"], node.successor["port"], {
                "type": "TRANSFER_KEYS",
                "req_id": "xfer_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "new_node": {
                        "ip": node.ip,
                        "port": node.port,
                        "id": node.node_id
                    }
                },
            })

            print(f"Joined ring (2-node). My pred={node.predecessor} "
                  f"succ={node.successor}")

        # -------------------------
        # NORMAL JOIN CASE
        # -------------------------
        else:
            succ = join_resp["data"]["successor"]
            node.successor = succ

            # 2) Ask successor for its predecessor
            pred_resp = send_request(succ["ip"], succ["port"], {
                "type": "GET_PREDECESSOR",
                "req_id": "get_pred_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {},
            })

            if pred_resp.get("status") != "OK":
                print("GET_PREDECESSOR failed:", pred_resp)
                raise SystemExit(1)

            pred = pred_resp["data"]["predecessor"]
            node.predecessor = pred

            # 3) Tell successor to set predecessor = me
            send_request(succ["ip"], succ["port"], {
                "type": "SET_PREDECESSOR",
                "req_id": "set_pred_succ_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "node": {
                        "ip": node.ip,
                        "port": node.port,
                        "id": node.node_id
                    }
                },
            })

            # 4) Tell predecessor to set successor = me
            send_request(pred["ip"], pred["port"], {
                "type": "SET_SUCCESSOR",
                "req_id": "set_succ_pred_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "node": {
                        "ip": node.ip,
                        "port": node.port,
                        "id": node.node_id
                    }
                },
            })

            # 5) Ask successor to transfer keys that now belong to me
            send_request(succ["ip"], succ["port"], {
                "type": "TRANSFER_KEYS",
                "req_id": "xfer_" + str(args.port),
                "origin": {"ip": args.ip, "port": args.port},
                "data": {
                    "new_node": {
                        "ip": node.ip,
                        "port": node.port,
                        "id": node.node_id
                    }
                },
            })

            print(f"Joined ring. My pred={node.predecessor} "
                  f"succ={node.successor}")

    # -------------------------
    # Always start server
    # -------------------------
    start_server(node, args.ip, args.port)
