import argparse
import json
import socket
import struct
import pathlib
import random
import sys
from typing import Tuple, List

FILE_SERVER_PORT = 9002

# --- TCP helper functions (from MP1 client.py / membership.py) ---

def send(sock, obj):
    """Serialize and send a JSON object."""
    data = json.dumps(obj).encode("utf-8")
    hdr = struct.pack("!I", len(data))
    sock.sendall(hdr + data)

def recv(sock, n):
    """Read n bytes from TCP socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf.extend(chunk)
    return bytes(buf)

def recv_msg(sock):
    """Receive a JSON message."""
    hdr = recv(sock, 4)
    (length,) = struct.unpack("!I", hdr)
    data = recv(sock, length)
    return json.loads(data.decode("utf-8"))

# --- HyDFS specific helper functions ---

def read_hosts_file(path: pathlib.Path) -> List[Tuple[str, int]]:
    """
    Read hosts.txt file and return list of (host, file_port) tuples.
    Automatically converts membership port (e.g., 9000) to file port (e.g., 9002).
    """
    hosts = []
    base_port_str = ""
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line:
            continue
        host, port_str = line.rsplit(":", 1)
        base_port_str = port_str  # Remember first port (e.g., "9000")
        
        # Our file port is membership port + 2
        file_port = int(port_str.strip()) + 2
        hosts.append((host.strip(), file_port))
    
    if not hosts and base_port_str:
        # Fallback for getfromreplica
        return [("dummy_host", int(base_port_str) + 2)]
        
    return hosts

def get_connection_info(
    command: str, 
    hosts_list: List[Tuple[str, int]], 
    vm_address: str = None
) -> Tuple[str, int]:
    """
    Determine which (host, port) to connect to based on command.
    """
    if command == "liststore":
        # liststore runs on "this" VM
        # We assume file port is port in hosts.txt + 2
        file_port = 9002  # Default
        if hosts_list:
            file_port = hosts_list[0][1]  # Infer file port from list
        return ("127.0.0.1", file_port)
        
    elif command == "getfromreplica":
        # getfromreplica connects to specific VM
        try:
            host, port_str = vm_address.rsplit(":", 1)
            file_port = int(port_str.strip()) + 2  # Port is always + 2
            return (host, file_port)
        except Exception:
            print(f"Error: Invalid VM address format '{vm_address}'. Must be 'host:port' (e.g., 'fa25-cs425-a801.cs.illinois.edu:9000')", file=sys.stderr)
            sys.exit(1)

    elif command == "create":
        return ("127.0.0.1", FILE_SERVER_PORT)
        
    else:
        # All other commands connect to a random node
        if not hosts_list:
            print("Error: hosts.txt is empty.", file=sys.stderr)
            sys.exit(1)
        return random.choice(hosts_list)

# --- Main function ---

def main():
    # 1. Set up HyDFS command line arguments
    parser = argparse.ArgumentParser(description="HyDFS Client [CS425 MP3]")
    parser.add_argument("--hosts", type=pathlib.Path, default=pathlib.Path("hosts.txt"), help="Path to hosts.txt file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # create
    create_parser = subparsers.add_parser("create", help="Create new file on HyDFS")
    create_parser.add_argument("localfilename", type=pathlib.Path, help="Local filename to upload")
    create_parser.add_argument("HyDFSfilename", type=str, help="Target filename on HyDFS")

    # get
    get_parser = subparsers.add_parser("get", help="Get file from HyDFS")
    get_parser.add_argument("HyDFSfilename", type=str, help="HyDFS filename to get")
    get_parser.add_argument("localfilename", type=pathlib.Path, help="Local filename to save to")
    
    # append
    append_parser = subparsers.add_parser("append", help="Append to file on HyDFS")
    append_parser.add_argument("localfilename", type=pathlib.Path, help="Local file containing content to append")
    append_parser.add_argument("HyDFSfilename", type=str, help="HyDFS target file to append to")

    # merge
    merge_parser = subparsers.add_parser("merge", help="Force merge all replicas of file")
    merge_parser.add_argument("HyDFSfilename", type=str, help="HyDFS filename to merge")
    
    # ls
    ls_parser = subparsers.add_parser("ls", help="List VMs where file is located")
    ls_parser.add_argument("HyDFSfilename", type=str, help="HyDFS filename to query")

    # liststore
    liststore_parser = subparsers.add_parser("liststore", help="List all files stored on this VM")
    
    # getfromreplica
    getrep_parser = subparsers.add_parser("getfromreplica", help="Get file from specific replica")
    getrep_parser.add_argument("VMaddress", type=str, help="Replica address (e.g., 'vm1:9000')")
    getrep_parser.add_argument("HyDFSfilename", type=str, help="HyDFS filename to get")
    getrep_parser.add_argument("localfilename", type=pathlib.Path, help="Local filename to save to")
    
    args = parser.parse_args()

    # 2. Read hosts.txt
    try:
        hosts_list = read_hosts_file(args.hosts)
    except Exception as e:
        print(f"Error reading {args.hosts}: {e}", file=sys.stderr)
        return
        
    # 3. Determine which (host, port) to connect to
    vm_address = getattr(args, "VMaddress", None)
    target_host, target_port = get_connection_info(args.command, hosts_list, vm_address)

    # 4. Prepare request
    req = {"command": args.command}
    
    try:
        if args.command == "create" or args.command == "append":
            # Check if local file exists
            if not args.localfilename.exists():
                print(f"Error: Local file {args.localfilename} not found.", file=sys.stderr)
                return
            # Read local file content (sending text content for simplicity)
            content = args.localfilename.read_text("utf-8")
            req["remote_file"] = args.HyDFSfilename
            req["content"] = content
        
        elif args.command in ["get", "merge", "ls", "getfromreplica"]:
            req["remote_file"] = args.HyDFSfilename
            # 'getfromreplica' is treated as 'get' on server side
            if args.command == "getfromreplica":
                req["command"] = "get"
        
        elif args.command == "liststore":
            pass  # Request is ready

    except Exception as e:
        print(f"Error preparing request: {e}", file=sys.stderr)
        return

    # 5. Establish TCP connection and send request
    print(f"[{args.command}] Connecting to {target_host}:{target_port}...", file=sys.stderr)
    try:
        with socket.create_connection((target_host, target_port), timeout=10) as s:
            s.settimeout(10)
            send(s, req)
            
            # 6. Receive final reply from server
            resp = recv_msg(s)
            
            # 7. Process response
            if resp.get("ok"):
                # Print command completion message
                print(f"Success: {args.command} operation completed on HyDFS.")

                # For create and append, list replicas
                if (args.command == "create" or args.command == "append") and "replicas" in resp:
                    print(f"  > File replicated to: {resp['replicas']}")
                
                # For get, write returned data to local file
                if args.command == "get" or args.command == "getfromreplica":
                    file_data = resp.get("file_data", "")
                    args.localfilename.write_text(file_data, encoding="utf-8")
                    print(f"  > File saved to {args.localfilename}")
                
                # For ls, print file ID and replicas
                if args.command == "ls":
                    print(f"  > File ID: {resp.get('file_id')}")
                    print(f"  > Replica locations: {resp.get('replicas')}")
                
                # For liststore, print files
                if args.command == "liststore":
                    print(f"  > Node ID: {resp.get('node_id')}")
                    print(f"  > Stored files: {resp.get('files')}")

            else:
                print(f"Error: Server returned error: {resp.get('error', 'Unknown error')}", file=sys.stderr)

    except socket.timeout:
        print(f"Error: Connection to {target_host}:{target_port} timed out.", file=sys.stderr)
    except ConnectionRefusedError:
        print(f"Error: Connection to {target_host}:{target_port} refused. Is daemon running?", file=sys.stderr)
    except Exception as e:
        print(f"Error: Could not connect or communicate with {target_host}:{target_port}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
