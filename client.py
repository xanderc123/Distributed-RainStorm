import argparse
import json
import socket
import struct
import concurrent.futures
import pathlib
import time
import re

DEFAULT_TIMEOUT = 10 #to ensure the code is fault tolerant 

def send(sock, obj): #serializes obj to json bytes
    data = json.dumps(obj).encode("utf-8")
    hdr = struct.pack("!I", len(data))
    sock.sendall(hdr + data)

def recv(sock, n): #reads n bytes from tcp 
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("connection closed")
        buf.extend(chunk)
    return bytes(buf)

def recv_msg(sock): 
    hdr = recv(sock, 4)
    (length,) = struct.unpack("!I", hdr)
    data = recv(sock, length)
    return json.loads(data.decode("utf-8"))

def read_hosts_file(path: pathlib.Path): #reads through hosts.txt and stores them
    hosts = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        host, port = line.rsplit(":", 1)
        hosts.append((host.strip(), int(port.strip())))
    return hosts

def query_host(host, port, pattern, options, timeout):
    req = {"pattern": pattern, "options": options}
    startTime = time.perf_counter()
    try:
        with socket.create_connection((host, port), timeout=timeout) as s: #creates tcp connection
            s.settimeout(timeout)
            send(s, req)
            resp = recv_msg(s)
            endTime = time.perf_counter()
            resp["total_time"] = endTime - startTime #to calcualte latency
            resp["host"] = resp.get("host", host)
            return (f"{host}:{port}", True, resp)
    except Exception as e:
        endTime = time.perf_counter()
        return (f"{host}:{port}", False, {"ok": False, "error": str(e), "elapsed_s": endTime - startTime})

def main():
    ap = argparse.ArgumentParser() #parses cli flags
    ap.add_argument("--hosts", type=pathlib.Path, default=pathlib.Path("hosts.txt"))
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    ap.add_argument("pattern")
    ap.add_argument("opts", nargs="*")
    ap.add_argument("--count", action="store_true")# count of matching lines per vm
    ap.add_argument("--sum", action="store_true") #total matching lines across all vms
    ap.add_argument("--outdir", type=pathlib.Path) #saves raw lines per server
    args = ap.parse_args()

    hosts = read_hosts_file(args.hosts)
    if not hosts:
        return

    options = list(args.opts)
    if "-H" not in options and "-h" not in options:
        options.append("-H")

    print(f"[client] querying {len(hosts)} hosts with pattern {args.pattern!r}")

    results = []

    #multithreaded to execute vm's parallely 
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(hosts)) as ex: 
        futs = [] 
        for h, p in hosts: #runs a query per host per thread
            fut = ex.submit(query_host, h, p, args.pattern, options, args.timeout)
            futs.append(fut)
        for f in concurrent.futures.as_completed(futs): #grab reusults of the fastes hosts first 
            results.append(f.result())
    
    total = 0
    latencies = []
    if args.outdir:
        args.outdir.mkdir(parents=True, exist_ok=True) #output folder
    for hostport, ok, resp in sorted(results, key=lambda x: x[0]):
        if not ok or not resp.get("ok"):
            print(f"\n===== {hostport}: ERROR =====")
            print(resp.get("error", "unknown error"))
            continue
        rc = resp.get("rc", 0)
        lines = resp.get("stdout","")
        err = resp.get("stderr","")
        if args.outdir is not None:
            fileName=re.sub(r"[^A-Za-z0-9._-]+", "_", hostport)
            out_path = args.outdir / f"{fileName}.txt"
            out_path.write_text(lines, encoding="utf-8")
        
        splitLines = lines.splitlines()
        print(f"\n===== {hostport}: rc={rc} =====")
        for ln in splitLines:
            print(ln)
        
        count = len(splitLines)
        total = total + count
        if (args.count):
            print(f"[count] {count}")

        if err:
            print(f"[stderr]\n{err}", end="")
        
        if "total_time" in resp:
            latencies.append(resp["total_time"]) #slowest VM completion time
    
    if (args.sum):
        print(f"\n[sum] {total}")

    if latencies:
        print(f"latency={max(latencies):.6f}")

    print(f"\n===== COMPLETED =====")

if __name__ == "__main__":
    main()