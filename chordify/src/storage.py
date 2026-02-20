import threading

class Storage:
   def __init__(self):
       # key_id -> {"key": str, "value": str, "is_replica": bool, "primary": {"ip":..,"port":..,"id":..} | None}
       self.data = {}
       self.lock = threading.Lock()

   # Primary insert: concat update (Chordify requirement)
   def insert_primary(self, key_id: int, key: str, value: str):
       with self.lock:
           if key_id in self.data:
               # concat update
               self.data[key_id]["value"] += "," + value
               self.data[key_id]["is_replica"] = False
           else:
               self.data[key_id] = {"key": key, "value": value, "is_replica": False, "primary": None}

   # Replica put: overwrite exactly (replicas should mirror primary state)
   def put_replica(self, key_id: int, key: str, value: str, primary: dict):
       with self.lock:
           self.data[key_id] = {"key": key, "value": value, "is_replica": True, "primary": primary}

   def query(self, key_id: int):
       with self.lock:
           return self.data.get(key_id)

   def delete(self, key_id: int):
       with self.lock:
           if key_id in self.data:
               del self.data[key_id]

   def get_all(self):
       with self.lock:
           return dict(self.data)

   def get_primary_only(self):
       # only records where this node is primary owner (not replicas)
       with self.lock:
           return {kid: rec for kid, rec in self.data.items() if rec.get("is_replica") is False}
