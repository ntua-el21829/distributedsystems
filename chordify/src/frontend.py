from flask import Flask, request
from net import send_request
import uuid
import math

app = Flask(__name__)

NODE_IP = "127.0.0.1"
NODE_PORT = 5000
consistency_mode = "unknown"
replication_factor = "?"
last_served_node = None
last_role = None
last_path = []


def make_request(msg_type, data):
    message = {
        "type": msg_type,
        "req_id": str(uuid.uuid4()),
        "origin": {"ip": "frontend", "port": 0},
        "data": data,
    }
    return send_request(NODE_IP, NODE_PORT, message)


def render_page(content="", ring_data=None):
    ring_html = ""
    nodes_table = ""

    if ring_data:
        size = 400
        center = size // 2
        radius = 130  # Σταθερή ακτίνα για τέλειο κύκλο
        num_nodes = len(ring_data)

        svg_elements = f'<circle cx="{center}" cy="{center}" r="{radius}" fill="none" stroke="#30363d" stroke-width="1" stroke-dasharray="5,5" />'
        nodes_divs = ""

        for i, node in enumerate(ring_data):
            # Υπολογισμός γωνίας για ομοιόμορφη κατανομή στον κύκλο
            angle = (2 * math.pi * i / num_nodes) - (math.pi / 2)
            x = center + radius * math.cos(angle)
            y = center + radius * math.sin(angle)

            node_class = "node-dot"

            # Highlight primary / replica
            if node.get("port") == last_served_node:
                if last_role == "primary":
                    node_class += " primary-node"
                elif last_role == "replica":
                    node_class += " replica-node"

            # Highlight routing path
            if node.get("port") in last_path:
                node_class += " path-node"

            nodes_divs += f"""
            <div class="{node_class}" style="left:{x}px; top:{y}px;">
                <div class="node-label">{node.get('port')}</div>
            </div>
            """

            # Προσθήκη στήλης Status στον πίνακα
            nodes_table += f"""
            <tr>
                <td>Node {i}</td>
                <td style="font-family:monospace; font-size:11px; color:#8b949e;">{node.get('id', 'N/A')}</td>
                <td>{node.get('port')}</td>
                <td style="color:#58a6ff;">{node.get('successor', {}).get('port', '-')}</td>
                <td style="color:#d29922;">{node.get('predecessor', {}).get('port', '-')}</td>
                <td><span class="status-pill">Active</span></td>
            </tr>
            """

        ring_html = f"""
        <div class="ring-wrapper">
            <svg width="{size}" height="{size}" style="position:absolute;">{svg_elements}</svg>
            <div class="nodes-container" style="width:{size}px; height:{size}px; position:relative;">
                {nodes_divs}
            </div>
        </div>
        """

    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Chordify 🎵</title>
        <style>
            :root {{
                --bg: #0d111a;
                --card: #161b22;
                --border: #30363d;
                --accent: #58a6ff;
                --green: #238636;
                --text: #e6edf3;
                --text-dim: #8b949e;
            }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                background: var(--bg);
                color: var(--text);
                margin: 0; padding: 40px;
                display: flex; flex-direction: column; align-items: center;
            }}
            .container {{ width: 100%; max-width: 1100px; }}
            h1 {{ font-size: 32px; font-weight: 600; margin: 0; }}
            .subtitle {{ color: var(--text-dim); margin: 10px 0 30px 0; font-size: 14px; }}
            .main-grid {{ display: grid; grid-template-columns: 380px 1fr; gap: 25px; }}
            .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 24px; }}
            h3 {{ margin-top: 0; font-weight: 600; font-size: 18px; color: var(--text); }}
            
            .op-group {{ margin-bottom: 20px; border-bottom: 1px solid #21262d; padding-bottom: 15px; }}
            .op-group:last-child {{ border: none; }}
            .op-title {{ font-size: 12px; font-weight: bold; color: var(--text-dim); text-transform: uppercase; margin-bottom: 10px; display: block; }}
            
            input {{
                padding: 10px; border-radius: 6px; border: 1px solid var(--border);
                background: #0d1117; color: white; width: 100%; margin-bottom: 8px; box-sizing: border-box;
            }}
            .btn {{ padding: 10px 15px; border-radius: 6px; border: none; font-weight: 600; cursor: pointer; width: 100%; }}
            .btn-green {{ background: var(--green); color: white; }}
            .btn-blue {{ background: #1f6feb; color: white; }}
            
            table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
            th {{ text-align: left; color: var(--text-dim); font-size: 11px; padding: 12px; border-bottom: 1px solid var(--border); }}
            td {{ padding: 12px; font-size: 13px; border-bottom: 1px solid #21262d; }}
            
            .ring-wrapper {{ position: relative; height: 400px; display: flex; justify-content: center; align-items: center; }}
            .node-dot {{
                position: absolute; width: 12px; height: 12px; background: var(--accent);
                border-radius: 50%; transform: translate(-50%, -50%);
                box-shadow: 0 0 15px var(--accent); border: 2px solid white;
                transition: all 0.3s ease;
            }}
            .node-label {{
                position: absolute; top: 18px; left: 50%; transform: translateX(-50%);
                font-size: 11px; background: rgba(0,0,0,0.7); padding: 2px 6px; border-radius: 4px; color: white;
            }}
            
            .status-pill {{
                background: rgba(46, 160, 67, 0.15); color: #3fb950;
                padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600;
            }}
            .primary-node {{
                background: #3fb950 !important;
                box-shadow: 0 0 20px #3fb950;
            }}

            .replica-node {{
                background: #d29922 !important;
                box-shadow: 0 0 20px #d29922;
            }}

            .path-node {{
                background: 3px solid #a371f7 !important;
                box-shadow: 0 0 15px #a371f7;
            }}
            .results-area {{ margin-top: 15px; padding: 12px; background: #0d1117; border-radius: 8px; border-left: 3px solid var(--accent); font-family: monospace; font-size: 12px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Chordify 🎵</h1>
            <div class="subtitle">
            Connected to {NODE_IP}:{NODE_PORT} — Distributed Hash Table Visualizer<br>
            Consistency: <b style="color:#58a6ff;">{consistency_mode.upper()}</b> |
            Replication Factor: <b style="color:#3fb950;">{replication_factor}</b>
            </div>
            <div class="main-grid">
                <div class="card">
                    <h3>Operations</h3>
                    <div class="op-group">
                        <span class="op-title">Insert</span>
                        <form method="post" action="/insert">
                            <input name="key" placeholder="Song title" required>
                            <input name="value" placeholder="Value" required>
                            <button type="submit" class="btn btn-green">Insert</button>
                        </form>
                    </div>
                    <div class="op-group">
                        <span class="op-title">Query</span>
                        <form method="post" action="/query">
                            <input name="key" placeholder="Song title or *" required>
                            <button type="submit" class="btn btn-blue">Query</button>
                        </form>
                    </div>
                    <div class="op-group">
                        <span class="op-title">Delete</span>
                        <form method="post" action="/delete">
                            <input name="key" placeholder="Song title" required>
                            <button type="submit" class="btn" style="background:#da3633; color:white;">Delete</button>
                        </form>
                    </div>
                    {f'<div class="results-area">{content}</div>' if content else ""}
                </div>

                <div class="card" style="display: flex; flex-direction: column; align-items: center;">
                    <div style="width: 100%; display:flex; justify-content:space-between;">
                        <h3>Ring Topology</h3>
                        <form method="get" action="/overlay">
                            <button type="submit" class="btn" style="background:transparent; color:var(--accent); border:1px solid var(--accent); width:auto; padding:5px 15px;">Visualize Ring</button>
                        </form>
                    </div>
                    {ring_html if ring_html else "<div style='margin: auto; color:var(--text-dim);'>No data visualized.</div>"}
                </div>

                <div class="card" style="grid-column: span 2;">
                    <h3>Node Details</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>NAME</th>
                                <th>ID (HASH)</th>
                                <th>PORT</th>
                                <th>SUCCESSOR</th>
                                <th>PREDECESSOR</th>
                                <th>STATUS</th>
                            </tr>
                        </thead>
                        <tbody>
                            {nodes_table if nodes_table else "<tr><td colspan='6' style='text-align:center;'>No data available</td></tr>"}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </body>
    </html>
    """


@app.route("/")
def home():
    return overlay()


@app.route("/insert", methods=["POST"])
def insert():
    resp = make_request(
        "INSERT", {"key": request.form["key"], "value": request.form["value"]}
    )

    if resp.get("status") != "OK":
        return render_page(f"Insert failed: {resp}")

    d = resp.get("data", {}) or {}

    # Eventual usually: stored_at + replicas
    stored_at = d.get("stored_at")
    replicas = d.get("replicas")

    # Linear chain commit: committed_at
    committed_at = d.get("committed_at")

    # Linear k=1: might include mode
    mode = d.get("mode")

    if committed_at is not None:
        msg = f"Write committed at node {committed_at} (linear chain)"
    elif stored_at is not None:
        if replicas is None:
            msg = f"Stored at node {stored_at}" + (f" ({mode})" if mode else "")
        else:
            msg = f"Stored at node {stored_at} with {replicas} replica(s)"
    else:
        # fallback – show raw data to not lie
        msg = f"Insert OK: {d}"

    return render_page(msg)


@app.route("/delete", methods=["POST"])
def delete():
    resp = make_request("DELETE", {"key": request.form["key"]})

    if resp.get("status") != "OK":
        return render_page("Delete failed")

    data = resp.get("data", {}) or {}

    deleted_at = data.get("deleted_at")

    if deleted_at is not None:
        return render_page(f"Deleted at node {deleted_at}")

    return render_page(f"Delete OK: {data}")


@app.route("/query", methods=["POST"])
def query():
    global last_served_node, last_role, last_path

    key = request.form["key"]
    resp = make_request("QUERY", {"key": key})

    if resp.get("status") != "OK":
        return render_page("Query failed")

    if key == "*":
        result = resp.get("data", {}).get("result", {})
        formatted = "<br>".join([f"{node}: {items}" for node, items in result.items()])
        return render_page(f"All Data:<br>{formatted}")

    result = resp.get("data", {}).get("result")
    served_by = resp.get("data", {}).get("served_by")

    if result is None:
        return render_page("Song Not Found")

    last_served_node = served_by
    last_role = "replica" if result["is_replica"] else "primary"
    last_path = resp.get("data", {}).get("path", [])

    formatted = f"""
    Value: {result['value']}<br>
    Role: {last_role.upper()}<br>
    Served by node: {served_by}
    """

    return render_page(formatted)


@app.route("/overlay")
def overlay():
    global consistency_mode, replication_factor

    resp = make_request("OVERLAY", {})
    ring = resp.get("data", {}).get("ring", [])

    enriched_ring = []

    for i, node in enumerate(ring):
        try:
            ping_resp = send_request(
                node["ip"],
                node["port"],
                {
                    "type": "PING",
                    "req_id": str(uuid.uuid4()),
                    "origin": {"ip": "frontend", "port": 0},
                    "data": {},
                },
            )

            node_info = ping_resp.get("data", {})

            # Παίρνουμε consistency/k από το πρώτο node
            if i == 0:
                consistency_mode = node_info.get("consistency", "unknown")
                replication_factor = node_info.get("replication_factor", "?")

            enriched_ring.append(
                {
                    "id": node_info.get("node_id"),
                    "port": node["port"],
                    "successor": node_info.get("successor"),
                    "predecessor": node_info.get("predecessor"),
                }
            )

        except:
            enriched_ring.append(
                {
                    "id": "ERROR",
                    "port": node["port"],
                    "successor": {},
                    "predecessor": {},
                }
            )

    return render_page(ring_data=enriched_ring)


if __name__ == "__main__":
    app.run(port=8000, debug=True)
