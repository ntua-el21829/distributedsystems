import socket
import json
import threading
import struct

# --- Framing helpers: 4-byte length prefix (big endian) ---

def _recv_exact(conn: socket.socket, n: int) -> bytes:
    """Receive exactly n bytes or raise ConnectionError."""
    chunks = []
    bytes_recd = 0
    while bytes_recd < n:
        chunk = conn.recv(n - bytes_recd)
        if not chunk:
            raise ConnectionError("Socket closed while receiving data")
        chunks.append(chunk)
        bytes_recd += len(chunk)
    return b"".join(chunks)


def recv_msg(conn: socket.socket) -> dict:
    """Receive one length-prefixed JSON message."""
    header = _recv_exact(conn, 4)
    (length,) = struct.unpack("!I", header)
    payload = _recv_exact(conn, length)
    return json.loads(payload.decode("utf-8"))


def send_msg(conn: socket.socket, message: dict) -> None:
    """Send one length-prefixed JSON message."""
    payload = json.dumps(message).encode("utf-8")
    header = struct.pack("!I", len(payload))
    conn.sendall(header + payload)


# --- Client side ---

def send_request(ip: str, port: int, message: dict, timeout: float = 3.0) -> dict:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((ip, port))
        send_msg(s, message)
        response = recv_msg(s)
        return response
    finally:
        s.close()


# --- Server side ---

def start_server(node, ip: str, port: int):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((ip, port))
    server.listen()

    print(f"Listening on {ip}:{port}")

    # --- REPAIR AFTER START (only if replication enabled) ---
    if getattr(node, "k", 1) > 1:
        try:
            node.handle_message({
                "type": "REPAIR_RING",
                "req_id": "repair_after_start",
                "origin": {"ip": node.ip, "port": node.port},
                "data": {"start_id": node.node_id},
            })
        except Exception as e:
            print("Repair failed at startup:", e)

    while True:
        conn, addr = server.accept()
        threading.Thread(
            target=handle_client,
            args=(node, conn),
            daemon=True
        ).start()


def handle_client(node, conn: socket.socket):
    try:
        message = recv_msg(conn)
        response = node.handle_message(message)
        send_msg(conn, response)
    except Exception as e:
        try:
            send_msg(conn, {"status": "ERROR", "error": str(e)})
        except Exception:
            pass
    finally:
        conn.close()