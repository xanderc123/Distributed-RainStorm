import argparse
import json
import socket
import struct
import subprocess
import threading
import os
import glob #for file name patterns

PORT = 9000 #tcp listening port 
PATTERN = "vm*.log"

def send(sock, obj): #serializes obj to json bytes
    data = json.dumps(obj).encode("utf-8")
    hdr = struct.pack("!I", len(data))
    sock.sendall(hdr + data) #sends header and payload 

def recv(sock, n): #reads n bytes from tcp 
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf.extend(chunk)
    return bytes(buf)

def recv_msg(sock):
    hdr = recv(sock, 4) #4-byte 
    (length,) = struct.unpack("!I", hdr)
    data = recv(sock, length)
    return json.loads(data.decode("utf-8")) #decodes json

def handle_client(conn, addr, logdir, grep_bin, timeout_s):
    try: #parsing request 
        req = recv_msg(conn) 
        pattern = req.get("pattern")
        options = req.get("options", [])

        if "-H" not in options and "-h" not in options:
            options.append("-H")

        if not pattern: #ensure pattern is there
            send(conn, {"ok": False, "error": "missing 'pattern'"})
            return

        files = sorted(glob.glob(PATTERN)) #finds files to search
        if not files:
            send(conn, {"ok": False, "error": f"no logs matching {PATTERN} under {logdir}"})
            return

        cmd = [grep_bin] + options + ["-e", pattern] + files #builds agrv for grep

        run = subprocess.run( #launches grep
            cmd, text=True, capture_output=True, timeout=timeout_s #timeout to kill grep if it hangs
        )
        send(conn, { 
            "ok": True,
            "host": socket.gethostname(),
            "rc": run.returncode, #0 is match; 1 is no match; 2 is error
            "stdout": run.stdout,
            "stderr": run.stderr
        })
    except Exception as e: #error handling
        try:
            send(conn, {"ok": False, "error": str(e)})
        except Exception:
            pass
    finally:
        conn.close() #closes client socket

def serve(port, logdir, grep_bin, timeout_s):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s: #creates listning tcp socket 
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", port))
        s.listen(128) #allows 128 connections
        print(f"[server] listening on 0.0.0.0:{port}, logdir={logdir}")
        while True: #one threaf per connection
            conn, addr = s.accept()
            t = threading.Thread(
                target=handle_client,
                args=(conn, addr, logdir, grep_bin, timeout_s),
                daemon=True #so thread can exit when main thread exits
            )
            t.start()

if __name__ == "__main__":
    ap = argparse.ArgumentParser() #for command line flags
    ap.add_argument("--port", type=int, default=PORT)
    ap.add_argument("--grep-bin", type=str, default="/bin/grep")
    ap.add_argument("--timeout", type=int, default=60) #timeoir for grep
    args = ap.parse_args()
    
    serve(args.port, ".", args.grep_bin, args.timeout) #starts the server; "." is the log directory