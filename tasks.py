import threading
import time
import socket
import re
import csv

class SourceThread(threading.Thread):
    def __init__(self, filepath, stage0_tasks, input_rate, logfile):
        super().__init__(daemon=True)
        self.filepath = filepath
        self.tasks = stage0_tasks
        self.input_rate = input_rate
        self.logfile = logfile
        self.running = True

    def log(self, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{timestamp}] [SOURCE] {msg}"
        with open(self.logfile, "a") as f:
            f.write(line + "\n")
        print(line, flush=True)

    def run(self):
        self.log("SourceThread started")

        try:
            with open(self.filepath, "r") as f:
                lines = f.readlines()
        except Exception as e:
            self.log(f"Cannot read source file: {e}")
            return

        if not self.tasks:
            self.log("ERROR: No stage-0 tasks available")
            return

        interval = 1 / self.input_rate
        idx = 0

        for idx, line in enumerate(lines[1:], 1):
            if not self.running:
                break

            data_tuple = (f"{self.filepath}:{idx},{line.strip()}")
            task = self.tasks[idx % len(self.tasks)]
            idx += 1

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

        self.log("SourceThread finished")


class TaskThread(threading.Thread):
    def __init__(self, task_id, operator, port, logfile, next_stage_tasks=None, ag_column=None):
        super().__init__(daemon=True)
        self.task_id = task_id
        self.operator = operator
        self.port = port
        self.logfile = logfile
        self.running = True
        self.next_stage_tasks = next_stage_tasks
        self.ag_column = ag_column

    def log(self, msg):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        # line = f"[{timestamp}] [Task {self.task_id}] {msg}"
        line = msg
        with open(self.logfile, "a") as f:
            f.write(line + "\n")
        print(line, flush=True)

    def filter_pass(self, line):
        # self.log(str(self.operator))
        # return False
        pattern = self.operator["args"]
        return re.search(pattern, line) is not None

    def parse_csv_line(self, line):
        return next(csv.reader([line]))

    def extract_pivot_value(self, line):
        parts = self.parse_csv_line(line)

        if self.ag_column < len(parts):
            key = parts[self.ag_column].strip()
            return key
        else:
            return ""

    def select_next_stage_task(self, key):
        # self.log(str(self.next_stage_tasks))
        if not self.next_stage_tasks:
            return None

        idx = hash(key) % len(self.next_stage_tasks)
        return self.next_stage_tasks[idx]

    def forward_tuple(self, line, dest):
        self.log("Forwarded!")
        # try:
        #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #     s.connect((dest["vm"], dest["port"]))
        #     s.sendall(line.encode())
        #     s.close()
        # except Exception as e:
        #     self.log(f"Routing error to {dest}: {e}")

    def run(self):
        self.log(f"Task thread started on data port {self.port}")

        # ---- minimal data listener for incoming tuples ----
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", self.port))
        server.listen(5)

        # simple infinite accept loop
        while self.running:
            try:
                conn, addr = server.accept()
                data = conn.recv(65535).decode("utf-8").strip()
                conn.close()

                if data:
                    key = data.strip().split(",")[0]
                    line = ",".join(data.strip().split(",")[1:])
                    if not self.filter_pass(line):
                        continue  # drop line
                    
                    # 3. SELECT DOWNSTREAM TASK
                    if self.next_stage_tasks and self.ag_column:
                        next_key = self.extract_pivot_value(line)
                        dest = self.select_next_stage_task(next_key)
                    
                    if dest is None:
                        self.log("No downstream tasks; cannot route.")
                        continue
                    # 4. FORWARD TO STAGE-2 TASK
                    self.forward_tuple(line, dest)
                    self.log(f"Forwarded tuple with key={next_key} to {dest['vm']}:{dest['port']}")


            except Exception as e:
                self.log(f"Error on task data port: {e}")
                time.sleep(0.2)
