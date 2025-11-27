# rainstorm_daemon.py
import argparse
import socket
import json
import time
import threading
from uuid import uuid4

class RainstormLeader:
    def __init__(self, logfile, host="0.0.0.0", port=9100):
        self.host = host
        self.port = port
        self.mp3_control_port = 9900
        self.logfile = logfile
        self.init_log()
        self.members = set()
        self.tasks =[]
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.port))

    def init_log(self):
        open(self.logfile, "w").close()
        
    def run(self):
        threading.Thread(target=self.membership_check_loop, daemon=True).start()
        self.log(f"[Leader] Listening for job submissions on {self.host}:{self.port}")
        self.listen()

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(self.logfile, "a") as f:
            f.write(message + "\n")
        print(f"[{timestamp}] {message}", flush=True)

    def sendJSON(self, obj: dict, addr_tuple) -> int:
        data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        sent = self.sock.sendto(data, addr_tuple)
        # self.bytes_out += sent
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
            members = [n for n in split_data if "fa25-cs425" in n]
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
        # TODO: Handling task rearrangement in case of membership change
    
    def list_tasks(self):
        return self.tasks

    def listen(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()

        while True:
            conn, addr = srv.accept()
            data = conn.recv(65535).decode("utf-8")

            try:
                msg = json.loads(data)
                
                if msg["command"] == "LIST_TASKS":
                    response = json.dumps(self.list_tasks())
                    conn.sendall(response.encode())
                elif msg["command"] == "KILL_TASKS":
                    pass
                elif msg["command"] == "SUBMIT_JOB":
                    self.log(f"[Leader] Received job: {msg}")
                    self.handle_job_submission(msg)
                    conn.sendall("".encode())
                else:
                    conn.sendall(b"ERROR: Unknown command")
                    conn.close()
            except Exception as e:
                print("[Leader] Invalid command received:", e)
            finally:
                conn.close()

    def handle_job_submission(self, job):
        self.log("[Leader] Handling job submission")
        self.log(str(job))

        Nstages = job["Nstages"]
        Ntasks = job["Ntasks_per_stage"]
        operators = job["operators"]

        workers = list(self.members)

        if not workers:
            self.log("[Leader] ERROR: No workers available!")
            return

        # Step 2: generate tasks
        task_assignments = []  # list of (task_id, vm_ip, port, operator_info)

        # PORT: IS IT UDP OR TCP
        next_port = 10000

        for stage in range(Nstages):
            op = operators[stage]

            for i in range(Ntasks):
                task_id = uuid4().int
                vm_ip = workers[(task_id) % len(workers)]

                task_info = {
                    "task_id": task_id,
                    "stage": stage + 1,
                    "vm": vm_ip,
                    "port": next_port,
                    "operator": op
                }

                task_assignments.append(task_info)

        # Step 3: save routing table
        self.tasks = task_assignments
        self.log(f"[Leader] Routing table: {self.tasks}")

        # Step 4: send START_TASK to each worker
        for t in task_assignments:
            self.send_start_task(t)

    def send_start_task(self, task):
        msg = {
            "command": "START_TASK",
            "task_id": task["task_id"],
            "stage": task["stage"],
            "port": task["port"],
            "operator": task["operator"]
        }

        vm = task["vm"]

        try:
            # s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # s.connect((vm, 9200))  # worker's control port
            # s.sendall(json.dumps(msg).encode())
            # s.close()

            self.log(f"[Leader] Sent START_TASK to {vm}: {msg}")

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
        # Create file if it doesn't exist
        with open(self.logfile, "a") as f:
            pass

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
        # Phase 1: Only print received task information to prove communication is successful
        task_id = task_info.get("task_id")
        operator = task_info.get("operator")
        self.log(f"[Worker] Received start task command! ID: {task_id}, Operator: {operator}")
        
        # Temporarily only log, don't actually start the process
        self.running_tasks[task_id] = task_info
        
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["leader", "worker"], required=True)
    parser.add_argument("--logfile", type=str)
    args = parser.parse_args()

    if args.mode == "leader":
        leader = RainstormLeader(logfile=args.logfile)
        leader.run()
    else:
        print("[Worker] Not implemented yet.")


if __name__ == "__main__":
    main()
