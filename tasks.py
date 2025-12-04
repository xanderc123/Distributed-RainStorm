import multiprocessing
import time
import socket
import re
import csv
import json
import struct
import os

# --- HyDFS Helper Functions (For Output) ---
def send_tcp_request(port, req):
    """Connects to the local HyDFS service and sends a request."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to localhost because the daemon/membership runs on the same node
        s.connect(("127.0.0.1", port)) 
        
        data = json.dumps(req).encode("utf-8")
        # Pack the length of the message as a 4-byte big-endian integer
        hdr = struct.pack("!I", len(data))
        s.sendall(hdr + data)
        
        # Receive response header (4 bytes length)
        resp_hdr = s.recv(4)
        if not resp_hdr: return None
        (resp_len,) = struct.unpack("!I", resp_hdr)
        
        # Receive response body
        resp_data = b""
        while len(resp_data) < resp_len:
            chunk = s.recv(resp_len - len(resp_data))
            if not chunk: break
            resp_data += chunk
            
        s.close()
        return json.loads(resp_data.decode("utf-8"))
    except Exception as e:
        # Silently fail or minimal log, as this runs frequently
        return None

def append_hydfs_file(filename, content):
    """Appends content to a HyDFS file via the local daemon."""
    req = {
        "command": "append", 
        "remote_file": filename, 
        "content": content
    }
    resp = send_tcp_request(9002, req)
    
    if not resp or not resp.get("ok"):
        create_req = {
            "command": "create",
            "remote_file": filename,
            "content": content
        }
        send_tcp_request(9002, create_req)

# ----------------------------------------------------

class SourceProcess(multiprocessing.Process):
    def __init__(self, filepath, stage0_tasks, input_rate, log_dir="."):
        super().__init__()
        self.filepath = filepath
        self.tasks = stage0_tasks
        self.input_rate = input_rate
        # Create a unique log file for this run
        self.log_file = os.path.join(log_dir, f"source_{int(time.time())}.log")
        self.running = True

    def log(self, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] [SOURCE] {msg}"
        with open(self.log_file, "a") as f:
            f.write(line + "\n")
        # Optional: Print to stdout for debugging, but might clutter
        # print(line, flush=True) 

    def run(self):
        # Important: Re-import socket inside process if needed, though usually fine.
        self.log(f"SourceProcess started. PID: {os.getpid()}")
        self.log(f"Reading Local File: {self.filepath}")

        try:
            with open(self.filepath, "r") as f:
                lines = f.readlines()
        except Exception as e:
            self.log(f"Cannot read source file: {e}")
            return

        if not self.tasks:
            self.log("ERROR: No stage-0 tasks available")
            return

        interval = 1.0 / max(1, self.input_rate)
        start_index = 1 
        
        # Main Loop
        for idx, line in enumerate(lines[start_index:], start_index):
            data_tuple = f"{self.filepath}:{idx}, {line.strip()}"
            
            task = self.tasks[idx % len(self.tasks)]
            vm = task["vm"]
            port = task["port"]

            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((vm, port))
                s.sendall(data_tuple.encode())
                s.close()
            except Exception as e:
                self.log(f"Failed to send to {vm}:{port} – {e}")

            time.sleep(interval)

        self.log("SourceProcess finished")


class TaskProcess(multiprocessing.Process):
    def __init__(self, task_id, operator, port, log_dir, next_stage_tasks=None, ag_column=None, dest_filename=None):
        super().__init__()
        self.task_id = task_id
        self.operator = operator
        self.port = port
        self.log_dir = log_dir
        self.next_stage_tasks = next_stage_tasks
        self.ag_column = ag_column
        self.dest_filename = dest_filename 
        
        # Determine Log Filename: task_<task_id>_<timestamp>.log
        self.log_file = os.path.join(self.log_dir, f"task_{self.task_id}_{int(time.time())}.log")
        
        # State will be initialized in run() to ensure process-safety
        self.state = {} 

    def log(self, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] {msg}"
        with open(self.log_file, "a") as f:
            f.write(line + "\n")
        # print(line, flush=True)

    def filter_pass(self, line):
        pattern = self.operator["args"]
        return re.search(pattern, line) is not None
    
    def parse_csv_line(self, line):
        try:
            return next(csv.reader([line]))
        except:
            return []

    def extract_pivot_value(self, line):
        parts = self.parse_csv_line(line)
        if parts and isinstance(self.ag_column, int) and self.ag_column < len(parts):
            key = parts[self.ag_column].strip()
            return key
        else:
            return "UNKNOWN"

    def select_next_stage_task(self, key):
        if not self.next_stage_tasks:
            return None
        idx = hash(key) % len(self.next_stage_tasks)
        return self.next_stage_tasks[idx]

    def forward_tuple(self, line, dest):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((dest["vm"], dest["port"]))
            s.sendall(line.encode())
            s.close()
        except Exception as e:
            self.log(f"Routing error to {dest}: {e}")

    def transform_operator(self, line):
        # Demo Application 2: Output fields 1-3
        # args format for demo might be different, but let's stick to spec
        # "Output fields 1-3 of the line"
        if self.operator["exe"] == "transform":
             # Demo-specific logic for App 2 if args say so, or generic
             # Let's assume generic transform for now based on args
             # But for Demo App 2, we might need a specific flag or flexible arg
             pass
        
        # Existing generic logic
        if isinstance(self.operator["args"], (list, tuple)):
            old_str, new_str = self.operator["args"]
            return line.replace(old_str, new_str)
        return line

    def aggregate_operator(self, key, line):
        try:
            col_idx = self.operator["args"]
            parts = self.parse_csv_line(line)
            if len(parts) > col_idx:
                agg_key = parts[col_idx].strip()
                if not agg_key: agg_key = "Empty" # Handle missing data
            else:
                agg_key = "Empty"
        except:
            agg_key = "Error"

        # Update state
        if agg_key not in self.state:
            self.state[agg_key] = 0
        self.state[agg_key] += 1
        
        current_count = self.state[agg_key]
        return f"{agg_key}, {current_count}"

    def run(self):
        # Re-initialize state here to be safe
        self.state = {}
        
        self.log(f"Task Process Started. PID: {os.getpid()}, Port: {self.port}")
        self.log(f"Operator: {self.operator}")

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(("0.0.0.0", self.port))
            server.listen(5)
        except Exception as e:
            self.log(f"FATAL: Could not bind to port {self.port}: {e}")
            return

        while True:
            try:
                conn, addr = server.accept()
                data = conn.recv(65535).decode("utf-8").strip()
                conn.close()

                if data:
                    parts = data.split(",", 1)
                    if len(parts) < 2: 
                        continue
                    
                    key = parts[0].strip()
                    line = parts[1].strip()
                    
                    op_type = self.operator["exe"]
                    processed_line = None
                    
                    if op_type == "filter":
                        if self.filter_pass(line):
                            processed_line = f"{key}, {line}" 
                    
                    elif op_type == "identity":
                        processed_line = f"{key}, {line}"
                    
                    elif op_type == "transform":
                        # For Demo App 2: "Output fields 1-3"
                        # We can hack this: if arg is "cut1-3", do the split
                        if self.operator.get("args") == "cut1-3":
                             csv_parts = self.parse_csv_line(line)
                             # Take first 3 fields
                             cut_res = ",".join(csv_parts[:3])
                             processed_line = f"{key}, {cut_res}"
                        else:
                             # Default replace logic
                             new_line = self.transform_operator(line)
                             processed_line = f"{key}, {new_line}"
                    
                    elif op_type == "aggregate":
                        processed_line = self.aggregate_operator(key, line)
                                    
                    if processed_line is None:
                        continue

                    # --- Routing / Output ---
                    
                    # Demo Requirement: "Log Each task’s output tuples"
                    self.log(f"[OUTPUT_TUPLE] {processed_line}")

                    if self.next_stage_tasks:
                        routing_key = key
                        if self.ag_column is not None and self.ag_column != "":
                            val_for_hash = self.extract_pivot_value(line)
                            routing_key = val_for_hash
                        dest = self.select_next_stage_task(routing_key)
                        if dest:
                            self.forward_tuple(processed_line, dest)
                    else:
                        # Final Stage Output
                        print(f"[OUTPUT] {processed_line}")
                        if self.dest_filename:
                            append_hydfs_file(self.dest_filename, processed_line + "\n")

            except Exception as e:
                self.log(f"Error on task data port: {e}")
                # Don't exit loop, just retry