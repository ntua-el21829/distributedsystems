import subprocess
import time
import argparse


def build_nodes(
    num_nodes: int, base_port: int, consistency: str, k: int, use_fingers: bool
):
    nodes = []

    # Bootstrap node
    bootstrap_cmd = [
        "python",
        "node.py",
        "--port",
        str(base_port),
        "--bootstrap",
        "--consistency",
        consistency,
        "--k",
        str(k),
    ]

    if use_fingers:
        bootstrap_cmd.append("--use-fingers")

    nodes.append(bootstrap_cmd)

    # Join nodes
    for i in range(1, num_nodes):
        port = base_port + i

        cmd = [
            "python",
            "node.py",
            "--port",
            str(port),
            "--bootstrap-ip",
            "127.0.0.1",
            "--bootstrap-port",
            str(base_port),
            "--consistency",
            consistency,
            "--k",
            str(k),
        ]

        if use_fingers:
            cmd.append("--use-fingers")

        nodes.append(cmd)

    return nodes


def main():
    parser = argparse.ArgumentParser(description="Chordify Node Launcher")

    parser.add_argument(
        "--nodes", type=int, default=4, help="Number of nodes (default: 4)"
    )
    parser.add_argument(
        "--base-port", type=int, default=5000, help="Starting port (default: 5000)"
    )
    parser.add_argument(
        "--consistency",
        choices=["linear", "eventual"],
        default="linear",
        help="Consistency mode (default: linear)",
    )
    parser.add_argument(
        "--k", type=int, default=3, help="Replication factor (default: 3)"
    )
    parser.add_argument(
        "--use-fingers", action="store_true", help="Enable finger table routing"
    )

    args = parser.parse_args()

    nodes = build_nodes(
        num_nodes=args.nodes,
        base_port=args.base_port,
        consistency=args.consistency,
        k=args.k,
        use_fingers=args.use_fingers,
    )

    processes = []

    for cmd in nodes:
        p = subprocess.Popen(cmd)
        processes.append(p)
        time.sleep(2.5)

    print("\nAll nodes started.")
    print(f"Nodes: {args.nodes}")
    print(f"Consistency: {args.consistency}")
    print(f"Replication factor k: {args.k}")
    print(f"Finger tables enabled: {args.use_fingers}\n")


if __name__ == "__main__":
    main()
