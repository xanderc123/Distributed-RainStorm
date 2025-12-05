import multiprocessing
import time
import socket
import re
import csv
import json
import struct
import os

# --- HyDFS Helper Functions (保持不变) ---
def send_tcp_request(port, req):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("127.0.0.1", port)) 
        data = json.dumps(req).encode("utf-8")
        hdr = struct.pack("!I", len(data))
        s.sendall(hdr + data)
        resp_hdr = s.recv(4)
        if not resp_hdr: return None
        (resp_len,) = struct.unpack("!I", resp_hdr)
        resp_data = b""
        while len(resp_data) < resp_len:
            chunk = s.recv(resp_len - len(resp_data))
            if not chunk: break
            resp_data += chunk
        s.close()
        return json.loads(resp_data.decode("utf-8"))
    except Exception:
        return None

def append_hydfs_file(filename, content):
    req = {"command": "append", "remote_file": filename, "content": content}
    resp = send_tcp_request(9002, req)
    if not resp or not resp.get("ok"):
        create_req = {"command": "create", "remote_file": filename, "content": content}
        send_tcp_request(9002, create_req)

# ----------------------------------------------------

class SourceProcess(multiprocessing.Process):
    def __init__(self, filepath, stage0_tasks, input_rate, log_dir="."):
        super().__init__()
        self.filepath = filepath
        self.tasks = stage0_tasks
        self.input_rate = input_rate
        self.log_file = os.path.join(log_dir, f"source_{int(time.time())}.log")
        self.running = True
        # --- 优化: Socket 缓存 ---
        self.sockets = {} # Key: (vm, port), Value: socket object

    def log(self, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a") as f:
            f.write(f"[{timestamp}] [SOURCE] {msg}\n")

    def get_socket(self, vm, port):
        """获取或创建持久连接"""
        key = (vm, port)
        if key in self.sockets:
            return self.sockets[key]
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((vm, port))
            self.sockets[key] = s
            return s
        except Exception as e:
            self.log(f"Connection failed to {vm}:{port}: {e}")
            return None

    def close_all_sockets(self):
        for s in self.sockets.values():
            try: s.close()
            except: pass
        self.sockets.clear()

    def run(self):
        self.log(f"SourceProcess started. PID: {os.getpid()}")
        try:
            with open(self.filepath, "r") as f:
                lines = f.readlines()
        except Exception as e:
            self.log(f"Cannot read source file: {e}")
            return

        if not self.tasks: return

        interval = 1.0 / max(1, self.input_rate)
        start_index = 1 
        last_time = time.time()
        
        for idx, line in enumerate(lines[start_index:], start_index):
            data_tuple = f"{self.filepath}:{idx}, {line.strip()}"
            task = self.tasks[idx % len(self.tasks)]
            
            s = self.get_socket(task["vm"], task["port"])
            if s:
                try:
                    s.sendall(data_tuple.encode() + b"\n")
                except Exception as e:
                    self.log(f"Send Error: {e}")
                    if (task["vm"], task["port"]) in self.sockets:
                        del self.sockets[(task["vm"], task["port"])]
            
            # --- 修复点: 只有 s 存在时才关闭 ---
            if s:
                try:
                    s.close()
                except: pass
                
            if (task["vm"], task["port"]) in self.sockets:
                del self.sockets[(task["vm"], task["port"])]
            # ----------------------------------

            now = time.time()
            if now - last_time < interval:
                time.sleep(interval - (now - last_time))
            last_time = time.time()

        self.log("SourceProcess finished")

class TaskProcess(multiprocessing.Process):
    def __init__(self, task_id, operator, port, log_dir, next_stage_tasks=None, ag_column=None, dest_filename=None, shared_counter=None):
        super().__init__()
        self.task_id = task_id
        self.operator = operator
        self.port = port
        self.log_dir = log_dir
        self.next_stage_tasks = next_stage_tasks
        self.ag_column = ag_column
        self.dest_filename = dest_filename 
        self.log_file = os.path.join(self.log_dir, f"task_{self.task_id}_{int(time.time())}.log")
        self.state = {} 
        self.shared_counter = shared_counter
        self.sockets = {} # 缓存下游连接

    def log(self, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a") as f:
            f.write(f"[{timestamp}] {msg}\n")

    def filter_pass(self, line):
        pattern = self.operator["args"]
        return re.search(pattern, line) is not None
    
    def parse_csv_line(self, line):
        try: return next(csv.reader([line]))
        except: return []

    def extract_pivot_value(self, line):
        parts = self.parse_csv_line(line)
        if parts and isinstance(self.ag_column, int) and self.ag_column < len(parts):
            return parts[self.ag_column].strip()
        return "UNKNOWN"

    def select_next_stage_task(self, key):
        if not self.next_stage_tasks: return None
        idx = hash(key) % len(self.next_stage_tasks)
        return self.next_stage_tasks[idx]

    # --- 优化: 持久化转发 ---
    def get_socket(self, vm, port):
        key = (vm, port)
        if key in self.sockets: return self.sockets[key]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((vm, port))
            self.sockets[key] = s
            return s
        except: return None

    def forward_tuple(self, line, dest):
        # 暂时使用短连接以保证稳定性，直到全链路支持长连接
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((dest["vm"], dest["port"]))
            s.sendall(line.encode())
            s.close()
        except Exception as e:
            self.log(f"Routing error: {e}")

    def transform_operator(self, line):
        if self.operator["exe"] == "transform":
             pass # Demo logic
        if isinstance(self.operator["args"], (list, tuple)):
            old_str, new_str = self.operator["args"]
            return line.replace(old_str, new_str)
        return line

    def aggregate_operator(self, key, line):
        try:
            col_idx = self.operator["args"]
            parts = self.parse_csv_line(line)
            agg_key = parts[col_idx].strip() if len(parts) > col_idx else "Empty"
        except: agg_key = "Error"

        if agg_key not in self.state: self.state[agg_key] = 0
        self.state[agg_key] += 1
        return f"{agg_key}, {self.state[agg_key]}"

    def run(self):
        self.state = {}
        self.log(f"Task Process Started. PID: {os.getpid()}, Port: {self.port}")

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(("0.0.0.0", self.port))
            server.listen(10) # 增加 backlog
        except Exception as e:
            self.log(f"FATAL: Bind error: {e}")
            return

        while True:
            try:
                conn, addr = server.accept()
                data = conn.recv(65535).decode("utf-8").strip()
                conn.close() # 

                if data:
                    if self.shared_counter:
                        with self.shared_counter.get_lock():
                            self.shared_counter.value += 1

                    parts = data.split(",", 1)
                    if len(parts) < 2: continue
                    key = parts[0].strip()
                    line = parts[1].strip()
                    
                    op_type = self.operator["exe"]
                    processed_line = None
                    
                    if op_type == "filter":
                        if self.filter_pass(line): processed_line = f"{key}, {line}" 
                    elif op_type == "identity":
                        processed_line = f"{key}, {line}"
                    elif op_type == "transform":
                        if self.operator.get("args") == "cut1-3":
                             csv_parts = self.parse_csv_line(line)
                             cut_res = ",".join(csv_parts[:3])
                             processed_line = f"{key}, {cut_res}"
                        else:
                             new_line = self.transform_operator(line)
                             processed_line = f"{key}, {new_line}"
                    elif op_type == "aggregate":
                        processed_line = self.aggregate_operator(key, line)
                                    
                    if processed_line is None: continue

                    self.log(f"[OUTPUT_TUPLE] {processed_line}")

                    if self.next_stage_tasks:
                        routing_key = key
                        if self.ag_column is not None and self.ag_column != "":
                            val_for_hash = self.extract_pivot_value(line)
                            routing_key = val_for_hash
                        dest = self.select_next_stage_task(routing_key)
                        if dest: self.forward_tuple(processed_line, dest)
                    else:
                        print(f"[OUTPUT] {processed_line}")
                        if self.dest_filename:
                            append_hydfs_file(self.dest_filename, processed_line + "\n")
            except Exception as e:
                self.log(f"Error: {e}")