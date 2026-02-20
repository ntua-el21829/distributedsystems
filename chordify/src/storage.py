import threading

class Storage:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def insert(self, key_id, key, value):
        with self.lock:
            if key_id in self.data:
                self.data[key_id]["value"] += "," + value
            else:
                self.data[key_id] = {"key": key, "value": value}

    def query(self, key_id):
        with self.lock:
            return self.data.get(key_id)

    def delete(self, key_id):
        with self.lock:
            if key_id in self.data:
                del self.data[key_id]

    def get_all(self):
        # Επιστρέφουμε copy για να μην δίνουμε reference που μπορεί να αλλάξει
        with self.lock:
            return dict(self.data)
