import multiprocessing
import time
import hashlib
import socket
import threading
import re
import csv
import json
import struct
import os
import pathlib

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

def compute_tuple_hash(tuple_str: str) -> str:
    return hashlib.sha1(tuple_str.encode("utf-8")).hexdigest()

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
        # Exactly once
        self.sent_ids = set()
        self.sent_log_path = os.path.join(log_dir, "source_sent_ids.log")

        if os.path.exists(self.sent_log_path):
            with open(self.sent_log_path, "r") as f:
                for line in f:
                    self.sent_ids.add(line.strip())


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

    def log_sent_tuple(self, tuple_str: str):
        h = compute_tuple_hash(tuple_str)

        with open(self.sent_log_path, "a") as f:
            f.write(h + "\n")

        self.sent_ids.add(h)

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
            
            # --- Try infinitely ---
            sent = False
            while not sent:
                s = self.get_socket(task["vm"], task["port"])
                if s:
                    try:
                        s.sendall(data_tuple.encode() + b"\n")
                        self.log_sent_tuple(data_tuple)
                        sent = True # 发送成功，跳出循环
                    except Exception:
                        # 发送失败，移除连接，下次循环重试
                        if (task["vm"], task["port"]) in self.sockets:
                            del self.sockets[(task["vm"], task["port"])]
                        try: s.close()
                        except: pass
                
                if not sent:
                    
                    time.sleep(0.1) 
            
            if (task["vm"], task["port"]) in self.sockets:
                s = self.sockets[(task["vm"], task["port"])]
                try: s.close()
                except: pass
                del self.sockets[(task["vm"], task["port"])]
            # ----------------------------------

            now = time.time()
            if now - last_time < interval:
                time.sleep(interval - (now - last_time))
            last_time = time.time()

        self.log("SourceProcess finished")

class TaskProcess(multiprocessing.Process):
    def __init__(self, task_id, operator, port, log_dir, next_stage_tasks=None, ag_column=None, dest_filename=None, shared_counter=None, failed_task_log_id=None):
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
        # 
        self.received_ids = set()
        self.sent_ids = set()
        self.received_log_path = os.path.join(self.log_dir, f"task_{self.task_id}_received.log")
        self.sent_log_path = os.path.join(self.log_dir, f"task_{self.task_id}_sent.log")
        self.received_hydfs_created = False
        self.sent_hydfs_created = False
        # To keep track of the last position of the log files
        self.received_last_pos = 0
        self.sent_last_pos = 0

        if failed_task_log_id:
            self.recover_failed_task_log(failed_task_log_id)

    def sync_log_to_hydfs(self, local_path, remote_filename, created_flag_attr, last_pos_attr):
        """
        Sync local log to HyDFS without duplicating data.
        - On first sync: CREATE with entire file content.
        - On later syncs: APPEND only new content since last sync.
        """

        if not os.path.exists(local_path):
            return

        created = getattr(self, created_flag_attr)
        last_pos = getattr(self, last_pos_attr)

        try:
            with open(local_path, "r", encoding="utf-8") as f:
                f.seek(last_pos)
                new_content = f.read()
                new_pos = f.tell()
        except Exception as e:
            self.log(f"[HydFS Sync] Failed reading {local_path}: {e}")
            return

        # If no new content and file was already created, skip
        if created and not new_content:
            return

        # If first upload (no remote file created)
        if not created:
            cmd = "create"
            content = pathlib.Path(local_path).read_text('utf-8')
            msg = {
                "command": "create",
                "remote_file": remote_filename,
                "local_file": os.path.basename(local_path),
                "content": content
            }
            self.log(f"[HydFS Sync] CREATE {remote_filename}")
            resp = send_tcp_request(9002, msg)

            if resp and resp.get("ok"):
                setattr(self, created_flag_attr, True)
                setattr(self, last_pos_attr, new_pos)
                self.log(f"[HydFS Sync] CREATE succeeded for {remote_filename}")
                return
            else:
                self.log(f"[HydFS Sync] CREATE failed for {remote_filename}")
                return

        # Otherwise append only new content
        if new_content:
            msg = {
                "command": "append",
                "remote_file": remote_filename,
                "local_file": os.path.basename(local_path),
                "content": new_content
            }
            self.log(f"[HydFS Sync] APPEND {remote_filename} ({len(new_content)} bytes)")
            resp = send_tcp_request(9002, msg)

            if resp and resp.get("ok"):
                setattr(self, last_pos_attr, new_pos)
                self.log(f"[HydFS Sync] APPEND succeeded for {remote_filename}")
            else:
                self.log(f"[HydFS Sync] APPEND failed for {remote_filename}")

    def hydfs_sync_loop(self):
        """
        Periodically sync both received and sent logs to HyDFS every 2 seconds.
        """
        remote_received = f"task_{self.task_id}_received.log"
        remote_sent = f"task_{self.task_id}_sent.log"

        while True:
            try:
                self.sync_log_to_hydfs(
                    self.received_log_path,
                    remote_received,
                    "received_hydfs_created",
                    "received_last_pos"
                )

                self.sync_log_to_hydfs(
                    self.sent_log_path,
                    remote_sent,
                    "sent_hydfs_created",
                    "sent_last_pos"
                )
            except Exception as e:
                self.log(f"[HydFS Sync] Error: {e}")

            time.sleep(2)

    def retrieve_hydfs_file(self, remote_filename):
        """
        Fetch a remote file from HyDFS using:
        {'command': 'get', 'remote_file': <filename>}
        Returns file content as a string, or "" if not found.
        Logs all events but never logs file content.
        """

        msg = {
            "command": "get",
            "remote_file": remote_filename
        }

        self.log(f"[HydFS Recovery] Requesting GET for {remote_filename}")

        resp = send_tcp_request(9002, msg)

        if resp and resp.get("ok"):
            self.log(f"[HydFS Recovery] GET succeeded for {remote_filename}")
            return resp.get("content", "")

        self.log(f"[HydFS Recovery] GET failed or file missing: {remote_filename}")
        return ""

    def recover_failed_task_log(self, failed_task_log_id):
        """
        Downloads the logs of the failed task (received + sent) from HyDFS
        and applies them as the current task's logs.
        """

        remote_received = f"task_{failed_task_log_id}_received.log"
        remote_sent     = f"task_{failed_task_log_id}_sent.log"

        local_received = self.received_log_path
        local_sent     = self.sent_log_path

        # 1. Retrieve logs from HyDFS
        rec_content = self.retrieve_hydfs_file(remote_received)
        sent_content = self.retrieve_hydfs_file(remote_sent)

        # 2. Overwrite THIS task’s local logs
        if rec_content:
            with open(local_received, "w") as f:
                f.write(rec_content)

        if sent_content:
            with open(local_sent, "w") as f:
                f.write(sent_content)

        # 2.5 Update local file-position trackers for sync loop
        if rec_content:
            self.received_last_pos = len(rec_content.encode("utf-8"))
        if sent_content:
            self.sent_last_pos = len(sent_content.encode("utf-8"))

        # 3. Refresh in-memory ID sets
        self.received_ids.clear()
        self.sent_ids.clear()

        if rec_content:
            for line in rec_content.splitlines():
                self.received_ids.add(line.strip())

        if sent_content:
            for line in sent_content.splitlines():
                self.sent_ids.add(line.strip())

        self.log(f"[Recovery] Restored logs from failed task {failed_task_log_id}")

    def load_id_logs(self):
        if os.path.exists(self.received_log_path):
            with open(self.received_log_path, "r") as f:
                for line in f:
                    self.received_ids.add(line.strip())

        if os.path.exists(self.sent_log_path):
            with open(self.sent_log_path, "r") as f:
                for line in f:
                    self.sent_ids.add(line.strip())

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
        sent = False
        while not sent:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((dest["vm"], dest["port"]))
                s.sendall(line.encode())
                s.close()
                sent = True
            except Exception:
                # 下游挂了，等待 0.1秒后重试，直到它复活
                time.sleep(0.1)

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

    def log_tuple_id(self, tuple_str: str, log_type: str):
        """
        log_type: "received" or "sent"
        """
        h = compute_tuple_hash(tuple_str)

        if log_type == "received":
            log_path = self.received_log_path
            id_set = self.received_ids
        elif log_type == "sent":
            log_path = self.sent_log_path
            id_set = self.sent_ids
        else:
            raise ValueError("log_type must be 'received' or 'sent'")

        with open(log_path, "a") as f:
            f.write(h + "\n")

        id_set.add(h)

        return h

    def run(self):
        self.load_id_logs()
        self.state = {}
        self.log(f"Task Process Started. PID: {os.getpid()}, Port: {self.port}")
        threading.Thread(target=self.hydfs_sync_loop, daemon=True).start()

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
                    # --- Exactly-once verification ---
                    h_in = compute_tuple_hash(data)
                    if h_in in self.received_ids:
                        continue
                    self.log_tuple_id(data, "received")

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
                    
                    # Hash and Log sent tuples
                    h_out = compute_tuple_hash(processed_line)
                    self.log_tuple_id(h_out, "sent")

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
