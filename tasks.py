import threading
import time
import socket
import re
import csv
import json
import struct
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
        print(f"[HyDFS Client Error] {e}")
        return None

def append_hydfs_file(filename, content):
    """Appends content to a HyDFS file via the local daemon."""
    # Try to append directly
    req = {
        "command": "append", 
        "remote_file": filename, 
        "content": content
    }
    # Send to membership.py file system port (default 9002)
    resp = send_tcp_request(9002, req)
    
    # If failed (e.g., file does not exist), try to create and write
    if not resp or not resp.get("ok"):
        create_req = {
            "command": "create",
            "remote_file": filename,
            "content": content
        }
        send_tcp_request(9002, create_req)

class TaskThread(threading.Thread):
    def __init__(self, task_id, operator, port, logfile, next_stage_tasks=None, ag_column=None, dest_filename=None):
        super().__init__(daemon=True)
        self.task_id = task_id
        self.operator = operator
        self.port = port
        self.logfile = logfile
        self.running = True
        self.next_stage_tasks = next_stage_tasks
        self.ag_column = ag_column
        # New: Stores the destination filename for the final output
        self.dest_filename = dest_filename 

    def log(self, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = msg
        with open(self.logfile, "a") as f:
            f.write(line + "\n")
        print(line, flush=True)

    def filter_pass(self, line):
        pattern = self.operator["args"]
        return re.search(pattern, line) is not None
    
    def identity_operator(self, line):
        return line

    def parse_csv_line(self, line):
        try:
            return next(csv.reader([line]))
        except:
            return []

    def extract_pivot_value(self, line):
        parts = self.parse_csv_line(line)
        # Ensure column index is valid
        if parts and isinstance(self.ag_column, int) and self.ag_column < len(parts):
            key = parts[self.ag_column].strip()
            return key
        else:
            return "UNKNOWN"

    def select_next_stage_task(self, key):
        if not self.next_stage_tasks:
            return None
        # Use Hash Partitioning to ensure stateful operations go to the same task
        # "use hash partitioning on the key modulo the number of tasks"
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
        # Example Transform: String Replacement
        # Assumes args is a list or tuple: [old_str, new_str]
        old_str, new_str = self.operator["args"]
        return line.replace(old_str, new_str)

    def aggregate_operator(self, key, line):
        # Placeholder for Stage 3 (Stateful aggregation)
        # We will implement the actual state storage later
        return f"{key}: 1" 

    def run(self):
        self.log(f"Task thread started on data port {self.port}")

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.port))
        server.listen(5)

        while self.running:
            try:
                conn, addr = server.accept()
                data = conn.recv(65535).decode("utf-8").strip()
                conn.close()

                if data:
                    # Parse RainStorm Tuple: "key, value"
                    # Format produced by Source: "filename:linenumber, line_content"
                    # Split only on the first comma
                    parts = data.split(",", 1)
                    if len(parts) < 2: 
                        continue
                    
                    key = parts[0].strip()
                    line = parts[1].strip() # This is the actual data content
                    
                    op_type = self.operator["exe"]
                    processed_line = None
                    
                    if op_type == "filter":
                        if self.filter_pass(line):
                            # Pass tuple forward preserving the format
                            processed_line = f"{key}, {line}" 
                        else:
                            continue # Tuple filtered out
                    
                    elif op_type == "identity":
                        processed_line = f"{key}, {line}"
                    
                    elif op_type == "transform":
                        # Transform usually modifies the Value, but Key (LineID) stays the same
                        new_line = self.transform_operator(line)
                        processed_line = f"{key}, {new_line}"
                    
                    elif op_type == "aggregate":
                        # Aggregate changes the Key (Group By Key)
                        # We extract the key and set value to 1 for now (to test flow)
                        agg_key = self.extract_pivot_value(line)
                        processed_line = f"{agg_key}, 1" 
                                    
                    if processed_line is None:
                        continue

                    # --- Routing Logic ---
                    dest = None
                    
                    # 1. If there is a next stage, route it
                    if self.next_stage_tasks:
                        routing_key = key # Default routing key
                        
                        # "key for the input to the Replace stage is the line"
                        # For aggregation, we MUST hash based on the content/value
                        if self.ag_column is not None and self.ag_column != "":
                            val_for_hash = self.extract_pivot_value(line)
                            routing_key = val_for_hash

                        dest = self.select_next_stage_task(routing_key)
                        
                        if dest:
                            self.forward_tuple(processed_line, dest)

                    # 2. If no next stage, this is the FINAL stage -> Output
                    else:
                        # Output to Console 
                        print(f"[OUTPUT] {processed_line}")
                        self.log(f"[OUTPUT] {processed_line}")
                        
                        # Output to HyDFS 
                        if self.dest_filename:
                            append_hydfs_file(self.dest_filename, processed_line + "\n")

            except Exception as e:
                self.log(f"Error on task data port: {e}")
                time.sleep(0.2)