# rainstorm_daemon.py
import argparse
import socket
import csv
import json
import time
import threading
import re
from uuid import uuid4

# Import modified threads from tasks.py
from tasks import SourceThread, TaskThread

class RainstormLeader:
    def __init__(self, logfile, host="0.0.0.0", port=9100):
        self.host = host
        self.port = port
        self.mp3_control_port = 9900
        self.logfile = logfile
        self.init_log()
        self.members = set()
        self.tasks = []
        # Keeps track of the ports assigned to different VMS. Uses 10K to 10050 range
        self.vm_next_port = {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.port))

    def init_log(self):
        open(self.logfile, "w").close()
        
    def run(self):
        threading.Thread(target=self.membership_check_loop, daemon=True).start()
        self.log(f"[Leader] Listening for job submissions on {self.host}:{self.port}")
        self.listen()

    def allocate_port_for_vm(self, vm_ip):
        base = 10000
        limit = 10050

        if vm_ip not in self.vm_next_port:
            # First port assignment for this VM
            self.vm_next_port[vm_ip] = base
            return base

        # Next available port
        next_port = self.vm_next_port[vm_ip] + 1

        if next_port > limit:
            raise RuntimeError(f"No free ports left for VM {vm_ip}")

        self.vm_next_port[vm_ip] = next_port
        return next_port

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(self.logfile, "a") as f:
            f.write(message + "\n")
        print(f"[{timestamp}] {message}", flush=True)

    def sendJSON(self, obj: dict, addr_tuple) -> int:
        data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        sent = self.sock.sendto(data, addr_tuple)
        return sent

    def retrieve_alive_members(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((self.host, 9900))
            s.sendall(b"LIST_MEM\n")
            data = s.recv(4096).decode("utf-8").strip()
            s.close()
            
            if not data:
                self.log("[Leader] MP3 returned empty membership list")
                return []

            split_data = data.split()
            # Filter for fa25-cs425 hosts
            members = [n.split(":")[0] for n in split_data if "fa25-cs425" in n]
            return members

        except Exception as e:
            self.log(f"[Leader] Failed to contact MP3 daemon: {e}")
            return []

    def membership_check_loop(self, interval=2):
        while True:
            new_members = set(self.retrieve_alive_members())
            if new_members != self.members:
                self.handle_tasks_on_membership_change(self.members, new_members)
                self.members = new_members
            time.sleep(interval)

    def handle_tasks_on_membership_change(self, old_members, new_members):
        self.log(f"Old: {old_members}")
        self.log(f"New: {new_members}")
        # TODO: Handling task rearrangement in case of membership change (Stage 3)
    
    def list_tasks(self):
        return self.tasks

    def listen(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()

        while True:
            conn, addr = srv.accept()
            try:
                data = conn.recv(65535).decode("utf-8")
                msg = json.loads(data)
                
                if msg["command"] == "LIST_TASKS":
                    response = json.dumps(self.list_tasks())
                    conn.sendall(response.encode())
                elif msg["command"] == "KILL_TASKS":
                    pass
                elif msg["command"] == "SUBMIT_JOB":
                    self.handle_job_submission(msg)
                    conn.sendall("OK".encode())
                else:
                    conn.sendall(b"ERROR: Unknown command")
            except Exception as e:
                print("[Leader] Invalid command or connection error:", e)
            finally:
                conn.close()

    def add_aggregate_column(self, operator):
        if operator["exe"] == "aggregate":
            return operator["args"]
        return None

    def handle_job_submission(self, job):
        self.log("[Leader] Handling job submission")
        self.log(str(job))

        Nstages = job["Nstages"]
        Ntasks = job["Ntasks_per_stage"]
        operators = job["operators"]
        # Extract the destination filename from the job request
        hydfs_dest_filename = job.get("hydfs_dest_filename") 

        workers = list(self.members)

        if not workers:
            self.log("[Leader] ERROR: No workers available!")
            return

        # Step 2: Generate tasks
        task_assignments = []  # list of (task_id, vm_ip, port, operator_info)

        for stage in range(Nstages):
            op = operators[stage]

            for i in range(Ntasks):
                task_id = uuid4().int
                vm_ip = workers[(task_id) % len(workers)] # Simple Round-Robin for now

                task_info = {
                    "task_id": task_id,
                    "stage": stage + 1,
                    "vm": vm_ip,
                    "port": self.allocate_port_for_vm(vm_ip),
                    "operator": op,
                    # If this is the source feeding into Stage 1, we need to know the aggregation column if Stage 1 is Agg
                    # Actually logic: if NEXT stage is agg, current stage needs to partition by that column.
                    # Current simplifiction: Source checks Stage 1.
                    "ag_column": self.add_aggregate_column(operators[stage + 1]) if stage == 0 and (stage + 1 < Nstages) else "",
                    # Pass the destination filename to the task info
                    "dest_filename": hydfs_dest_filename
                }

                task_assignments.append(task_info)

        # Step 3: Update local task table
        self.tasks += task_assignments

        # Step 4: Send START_TASK to each worker
        for t in task_assignments:
            self.send_start_task(t)

        # --- START SOURCE FOR STAGE 0 ---
        # Identify stage 1 tasks to feed data into
        stage0_tasks = [t for t in task_assignments if t["stage"] == 1]
        src_file = job["hydfs_src_directory"]
        input_rate = job["input_rate"]

        # Start the Source Thread on the Leader
        source = SourceThread(src_file, stage0_tasks, input_rate, self.logfile)
        source.start()
        self.log("[Leader] Source thread started")

    def send_start_task(self, task):
        msg = {
            "command": "START_TASK",
            "task_id": task["task_id"],
            "stage": task["stage"],
            "port": task["port"],
            "operator": task["operator"],
            "ag_column": task["ag_column"],
            # Include dest_filename in the message to the worker
            "dest_filename": task["dest_filename"]
        }

        vm = task["vm"]

        # If this is Stage 1, it needs to know about Stage 2 tasks for routing
        if task["stage"] == 1:
            stage2 = [t for t in self.tasks if t["stage"] == 2]
            msg["next_stage_tasks"] = [
                {"vm": t["vm"], "port": t["port"], "task_id": t["task_id"]}
                for t in stage2
            ]

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((vm, 9200)) # 9200 is the Worker listening port
            s.sendall(json.dumps(msg).encode())
            s.close()
        except Exception as e:
            self.log(f"[Leader] Failed to contact worker {vm}: {e}")

class RainstormWorker:

    def __init__(self, logfile, host="0.0.0.0", port=9200):
        self.host = host
        self.port = port
        self.logfile = logfile
        self.init_log()
        self.running_tasks = {}  # Store currently running task information
        
    def init_log(self):
        open(self.logfile, "w").close()

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_msg = f"[{timestamp}] {message}"
        with open(self.logfile, "a") as f:
            f.write(log_msg + "\n")
        print(log_msg, flush=True)

    def run(self):
        self.log(f"[Worker] Started. Listening on {self.host}:{self.port}")
        self.listen()

    def listen(self):
        # Start TCP Server to listen for Leader's commands
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)

        while True:
            conn, addr = server.accept()
            # Start a thread for each connection to avoid blocking
            t = threading.Thread(target=self.handle_message, args=(conn, addr), daemon=True)
            t.start()

    def handle_message(self, conn, addr):
        try:
            data = conn.recv(65535).decode("utf-8")
            if not data:
                return
            
            msg = json.loads(data)
            command = msg.get("command")

            if command == "START_TASK":
                self.handle_start_task(msg)
                # Reply to Leader acknowledging receipt of command
                conn.sendall(json.dumps({"status": "OK"}).encode())
            else:
                self.log(f"[Worker] Unknown command received: {command}")

        except Exception as e:
            self.log(f"[Worker] Error handling message from {addr}: {e}")
        finally:
            conn.close()

    def handle_start_task(self, task_info):
        task_id = task_info.get("task_id")
        operator = task_info.get("operator")
        port = task_info.get("port")
        next_stage_tasks = task_info.get("next_stage_tasks")
        ag_column = task_info.get("ag_column")
        # Extract dest_filename from the message
        dest_filename = task_info.get("dest_filename")

        # Create TaskThread with the new dest_filename argument
        t = TaskThread(task_id, operator, port, self.logfile, next_stage_tasks, ag_column, dest_filename)
        t.start()

        self.running_tasks[task_id] = {
            "info": task_info,
            "thread": t,
            "status": "running"
        }

        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["leader", "worker"], required=True)
    parser.add_argument("--logfile", type=str)
    args = parser.parse_args()

    if args.mode == "leader":
        leader = RainstormLeader(logfile=args.logfile)
        leader.run()
    if args.mode == "worker":
        worker = RainstormWorker(logfile=args.logfile)
        worker.run()
    else:
        print("[Worker] Not implemented yet.")


if __name__ == "__main__":
    main()