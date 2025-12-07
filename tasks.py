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

# --- HyDFS Helper Functions ---
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
        self.tasks = stage0_tasks # 这是一个 Shared List (Proxy)
        self.input_rate = input_rate
        self.log_file = os.path.join(log_dir, f"source_{int(time.time())}.log")
        self.running = True
        self.sockets = {} 
        # Exactly once logging
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
        key = (vm, port)
        if key in self.sockets: return self.sockets[key]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect((vm, port))
            self.sockets[key] = s
            return s
        except: return None

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
            
            # --- 【关键修改 1】无限重试 + 动态路由读取 ---
            sent = False
            while not sent:
                # 每次循环都重新读取 self.tasks (共享列表)
                # 这样如果 Leader 更改了路由，这里能拿到新的 VM 地址
                try:
                    current_tasks = self.tasks 
                    if not current_tasks: break
                    task = current_tasks[idx % len(current_tasks)]
                except:
                    # 列表访问失败（极少见），稍后重试
                    time.sleep(0.1); continue

                s = self.get_socket(task["vm"], task["port"])
                if s:
                    try:
                        s.sendall(data_tuple.encode() + b"\n")
                        self.log_sent_tuple(data_tuple)
                        sent = True # 成功发送，退出循环
                    except Exception:
                        # 发送失败，移除连接，下次循环重试
                        if (task["vm"], task["port"]) in self.sockets:
                            del self.sockets[(task["vm"], task["port"])]
                        try: s.close()
                        except: pass
                
                # 为了配合 Task 端的短连接逻辑，这里必须关闭
                if s:
                    try: s.close()
                    except: pass
                if (task["vm"], task["port"]) in self.sockets:
                    del self.sockets[(task["vm"], task["port"])]

                if not sent:
                    # 发送失败，等待路由更新
                    time.sleep(0.1) 
            # ------------------------------------------

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
        self.next_stage_tasks = next_stage_tasks # Shared List (Proxy)
        self.ag_column = ag_column
        self.dest_filename = dest_filename 
        self.log_file = os.path.join(self.log_dir, f"task_{self.task_id}_{int(time.time())}.log")
        self.state = {} 
        self.shared_counter = shared_counter
        self.sockets = {}
        
        # Exactly-once logs
        self.received_ids = set()
        self.sent_ids = set()
        self.received_log_path = os.path.join(self.log_dir, f"task_{self.task_id}_received.log")
        self.sent_log_path = os.path.join(self.log_dir, f"task_{self.task_id}_sent.log")
        self.received_hydfs_created = False
        self.sent_hydfs_created = False
        self.received_last_pos = 0
        self.sent_last_pos = 0

        if failed_task_log_id:
            self.recover_failed_task_log(failed_task_log_id)

    def sync_log_to_hydfs(self, local_path, remote_filename, created_flag_attr, last_pos_attr):
        if not os.path.exists(local_path): return
        created = getattr(self, created_flag_attr)
        last_pos = getattr(self, last_pos_attr)
        try:
            with open(local_path, "r", encoding="utf-8") as f:
                f.seek(last_pos)
                new_content = f.read()
                new_pos = f.tell()
        except: return

        if created and not new_content: return

        if not created:
            content = pathlib.Path(local_path).read_text('utf-8')
            msg = {"command": "create", "remote_file": remote_filename, "local_file": os.path.basename(local_path), "content": content}
            resp = send_tcp_request(9002, msg)
            if resp and resp.get("ok"):
                setattr(self, created_flag_attr, True)
                setattr(self, last_pos_attr, new_pos)
            return

        if new_content:
            msg = {"command": "append", "remote_file": remote_filename, "local_file": os.path.basename(local_path), "content": new_content}
            resp = send_tcp_request(9002, msg)
            if resp and resp.get("ok"):
                setattr(self, last_pos_attr, new_pos)

    def hydfs_sync_loop(self):
        remote_rec = f"task_{self.task_id}_received.log"
        remote_sent = f"task_{self.task_id}_sent.log"
        while True:
            try:
                self.sync_log_to_hydfs(self.received_log_path, remote_rec, "received_hydfs_created", "received_last_pos")
                self.sync_log_to_hydfs(self.sent_log_path, remote_sent, "sent_hydfs_created", "sent_last_pos")
            except: pass
            time.sleep(2)

    def retrieve_hydfs_file(self, remote_filename):
        msg = {"command": "get", "remote_file": remote_filename}
        resp = send_tcp_request(9002, msg)
        return resp.get("content", "") if resp and resp.get("ok") else ""

    def recover_failed_task_log(self, failed_task_log_id):
        r_con = self.retrieve_hydfs_file(f"task_{failed_task_log_id}_received.log")
        s_con = self.retrieve_hydfs_file(f"task_{failed_task_log_id}_sent.log")
        
        if r_con:
            with open(self.received_log_path, "w") as f: f.write(r_con)
            self.received_last_pos = len(r_con.encode("utf-8"))
            for line in r_con.splitlines(): self.received_ids.add(line.strip())
            
        if s_con:
            with open(self.sent_log_path, "w") as f: f.write(s_con)
            self.sent_last_pos = len(s_con.encode("utf-8"))
            for line in s_con.splitlines(): self.sent_ids.add(line.strip())
            
        self.log(f"[Recovery] Restored logs from {failed_task_log_id}")

    def load_id_logs(self):
        if os.path.exists(self.received_log_path):
            with open(self.received_log_path, "r") as f:
                for line in f: self.received_ids.add(line.strip())
        if os.path.exists(self.sent_log_path):
            with open(self.sent_log_path, "r") as f:
                for line in f: self.sent_ids.add(line.strip())

    def log(self, msg):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.log_file, "a") as f: f.write(f"[{ts}] {msg}\n")

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

    # --- 【关键修改 2】从 Shared List 读取最新路由 + MD5 哈希 ---
    def select_next_stage_task(self, key):
        try:
            current_next = self.next_stage_tasks
            if not current_next: return None
            # Stable Hashing (Test 1 计数合并关键)
            hash_val = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
            idx = hash_val % len(current_next)
            return current_next[idx]
        except: return None

    # --- 【关键修改 3】forward_tuple 改为返回 True/False ---
    def forward_tuple(self, line, dest):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((dest["vm"], dest["port"]))
            s.sendall(line.encode())
            s.close()
            return True # 发送成功
        except Exception:
            return False # 发送失败

    def transform_operator(self, line):
        if self.operator["exe"] == "transform":
             if self.operator.get("args") == "cut1-3": return line
        if isinstance(self.operator["args"], (list, tuple)):
            return line.replace(self.operator["args"][0], self.operator["args"][1])
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
        h = compute_tuple_hash(tuple_str)
        path = self.received_log_path if log_type == "received" else self.sent_log_path
        ids = self.received_ids if log_type == "received" else self.sent_ids
        with open(path, "a") as f: f.write(h + "\n")
        ids.add(h)
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
            server.listen(10)
        except Exception as e:
            self.log(f"FATAL: Bind error: {e}")
            return

        while True:
            try:
                conn, addr = server.accept()
                data = conn.recv(65535).decode("utf-8").strip()
                conn.close()

                if data:
                    h_in = compute_tuple_hash(data)
                    if h_in in self.received_ids: continue
                    self.log_tuple_id(data, "received")

                    if self.shared_counter:
                        with self.shared_counter.get_lock(): self.shared_counter.value += 1

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
                    h_out = compute_tuple_hash(processed_line)
                    self.log_tuple_id(h_out, "sent")

                    if self.next_stage_tasks:
                        routing_key = key
                        if self.ag_column is not None and self.ag_column != "":
                            val_for_hash = self.extract_pivot_value(line)
                            routing_key = val_for_hash
                        
                        # --- 【关键修改 4】路由无限重试 ---
                        sent = False
                        while not sent:
                            # 重新选择目标 (获取最新地址)
                            dest = self.select_next_stage_task(routing_key)
                            if not dest:
                                time.sleep(0.1); continue
                            
                            # 尝试转发
                            if self.forward_tuple(processed_line, dest):
                                sent = True
                            else:
                                # 失败则等待
                                time.sleep(0.1)
                        # -----------------------------
                    else:
                        print(f"[OUTPUT] {processed_line}")
                        if self.dest_filename:
                            append_hydfs_file(self.dest_filename, processed_line + "\n")
            except Exception as e:
                self.log(f"Error: {e}")