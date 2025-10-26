import argparse, socket, sys, pathlib

def main():
    ap = argparse.ArgumentParser(description = "Control client for membershipD")
    ap.add_argument("--host", default = "127.0.0.1", help = "control host (defaults to localhost)")
    ap.add_argument("--port", type = int, default = 9900, help = "control port (defaults to 9900)")
    ap.add_argument("cmd", choices=[
        "list_mem", "list_self", "join", "leave", "display_suspects", 
        "switch", "display_protocol", "drop", "list_mem_ids"
    ], help="command")
    ap.add_argument("--timeout", type = float, default = 3.0, help = "socket timeout (s)")
    ap.add_argument("args", nargs="*", help="arguments for commands like switch or drop")

    args = ap.parse_args()

    line = None
    if args.cmd == "drop":
        if len(args.args) != 1:
            print("error: drop requires exactly one float argument between 0 and 1", file=sys.stderr)
            sys.exit(2)
        try:
            p = float(args.args[0])
        except Exception as e:
            print("error: drop requires a float between 0 and 1", file = sys.stderr)
            sys.exit(2)
        if not (0.0 <= p <= 1.0):
                print("error: drop must be in [0, 1]", file = sys.stderr)
                sys.exit(2)
        line = f"DROP {p}\n"

    elif args.cmd == "leave":
        line = "LEAVE\n"

    elif args.cmd == "list_mem":
        line = "LIST_MEM\n"
    
    elif args.cmd == "list_mem_ids":
        line = "LIST_MEM_IDS\n"

    elif args.cmd == "list_self":
        line = "LIST_SELF\n"

    elif args.cmd == "join":
        line = "JOIN\n"

    elif args.cmd == "display_suspects":
        line = "DISPLAY_SUSPECTS\n"

    elif args.cmd == "display_protocol":
        line = "DISPLAY_PROTOCOL\n"

    elif args.cmd == "switch":  
        if len(args.args) != 2:
            print("error: switch requires exactly 2 parameters", file=sys.stderr)
            sys.exit(2)
        mode_param, suspicion_param = args.args
        if mode_param not in ("gossip", "ping"):
            print("Error: first parameter must be 'gossip' or 'ping'", file=sys.stderr)
            sys.exit(2)
        if suspicion_param not in ("suspect", "nosuspect"):
            print("Error: second parameter must be 'suspect' or 'nosuspect'", file=sys.stderr)
            sys.exit(2)
        line = f"SWITCH {mode_param} {suspicion_param}\n"
        
        hosts_all = pathlib.Path("hosts.txt")
        hosts_read = read_hosts_file(hosts_all)
        this_host = socket.gethostname()
        for host, _ in hosts_read:
            try:
                if host == this_host:
                    target = "127.0.0.1"
                else:
                    target = host

                send_control(target, line)
                print(f"Sent SWITCH to {host}")
            except Exception as e:
                print(f"Failed to contact {host}: {e}")

    try:
        with socket.create_connection((args.host, args.port), timeout = args.timeout) as s:
            s.sendall(line.encode("utf-8"))
            s.shutdown(socket.SHUT_WR)
            data = s.recv(4096)
            if data:
                sys.stdout.write(data.decode("utf-8"))

    except Exception as e:
        print(f"control error: {e}", file = sys.stderr)
        sys.exit(1)

def read_hosts_file(path: pathlib.Path): #reads through hosts.txt and stores them
    hosts = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        host, port = line.rsplit(":", 1)
        hosts.append((host.strip(), int(port.strip())))
    return hosts

def send_control(host, line):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    sock.connect((host, 9900))
    sock.sendall(line.encode("utf-8"))
    response = sock.recv(4096)
    sock.close()
    return response


if __name__ == "__main__":
    main()
    