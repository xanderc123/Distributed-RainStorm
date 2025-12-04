import argparse
import socket
import csv
import json
import time
import threading
import os
import signal
from uuid import uuid4

# Import modified Processes from tasks.py
from tasks import SourceProcess, TaskProcess

class RainstormLeader:
    def __init__(self, logfile, host="0.0.0.0", port=9100):
        self.host = host
        self.port = port
        self.logfile = logfile
        self.init_log()
        self.members = set()
        self.tasks = [] # List of task dicts
        self.vm_next_port = {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.port))

    def init_log(self):
        open(self.logfile, "w").close()
        # Demo Requirement: Start/End of run
        self.log("[Leader] RainStorm System Started")
        
    def run(self):
        threading.Thread(target=self.membership_check_loop, daemon=True).start()
        # Demo Requirement: Log rates every second
        threading.Thread(target=self.rate_monitor_loop, daemon=True).start()
        
        self.log(f"[Leader] Listening for job submissions on {self.host}:{self.port}")
        self.listen()

    def allocate_port_for_vm(self, vm_ip):
        base = 10000
        limit = 10050
        if vm_ip not in self.vm_next_port:
            self.vm_next_port[vm_ip] = base
            return base
        next_port = self.vm_next_port[vm_ip] + 1
        if next_port > limit:
            raise RuntimeError(f"No free ports left for VM {vm_ip}")
        self.vm_next_port[vm_ip] = next_port
        return next_port

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(self.logfile, "a") as f:
            f.write(f"[{timestamp}] {message}\n")
        print(f"[{timestamp}] {message}", flush=True)

    def sendJSON(self, obj: dict, addr_tuple) -> int:
        data = json.dumps(obj).encode("utf-8")
        return self.sock.sendto(data, addr_tuple)

    def retrieve_alive_members(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((self.host, 9900))
            s.sendall(b"LIST_MEM\n")
            data = s.recv(4096).decode("utf-8").strip()
            s.close()
            if not data: return []
            split_data = data.split()
            # Filter for fa25-cs425 hosts
            members = [n.split(":")[0] for n in split_data if "fa25-cs425" in n]
            return members
        except Exception:
            return []

    def membership_check_loop(self, interval=2):
        while True:
            new_members = set(self.retrieve_alive_members())
            if new_members != self.members:
                self.handle_tasks_on_membership_change(self.members, new_members)
                self.members = new_members
            time.sleep(interval)

    def handle_tasks_on_membership_change(self, old_members, new_members):
        failed_nodes = old_members - new_members
        if not failed_nodes: return

        self.log(f"[FailDetect] Failed nodes: {failed_nodes}")
        active_workers = list(new_members)
        if not active_workers:
            self.log("[Leader] Critical: No active workers left!")
            return

        reassigned_count = 0
        for task in self.tasks:
            if task["vm"] in failed_nodes:
                old_vm = task["vm"]
                new_vm = active_workers[task["task_id"] % len(active_workers)]
                new_port = self.allocate_port_for_vm(new_vm)
                
                self.log(f"[Reschedule] Task {task['task_id']} (Stage {task['stage']}) {old_vm} -> {new_vm}")
                
                task["vm"] = new_vm
                task["port"] = new_port
                # Reset PID info as it's a new start
                task["pid"] = None 
                
                self.send_start_task(task)
                reassigned_count += 1
        
        if reassigned_count > 0:
            self.update_routing_tables()

    def update_routing_tables(self):
        for task in self.tasks:
            self.send_start_task(task)

    def rate_monitor_loop(self):
        # Placeholder for Rate Monitoring (Step 3)
        while True:
            time.sleep(1)
            # Todo: Aggregate stats from workers
            pass

    def list_tasks(self):
        # Demo Requirement: Output VM, PID, op_exe, and local log file
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
                
                elif msg["command"] == "SUBMIT_JOB":
                    self.handle_job_submission(msg)
                    conn.sendall("OK".encode())
                
                elif msg["command"] == "UPDATE_PID":
                    # Worker reporting back PID and Logfile
                    task_id = msg["task_id"]
                    pid = msg["pid"]
                    logfile = msg["logfile"]
                    
                    # Update local record
                    for t in self.tasks:
                        if t["task_id"] == task_id:
                            t["pid"] = pid
                            t["logfile"] = logfile
                            self.log(f"[TaskStarted] Task {task_id} on {t['vm']} PID: {pid} Log: {logfile}")
                            break
                    conn.sendall("OK".encode())

            except Exception as e:
                print("[Leader] Error:", e)
            finally:
                conn.close()

    def handle_job_submission(self, job):
        self.log(f"[Leader] Job Submitted: {job}")
        
        Nstages = job["Nstages"]
        Ntasks = job["Ntasks_per_stage"]
        operators = job["operators"]
        hydfs_dest_filename = job.get("hydfs_dest_filename") 

        workers = list(self.members)
        if not workers:
            self.log("[Leader] ERROR: No workers available!")
            return

        task_assignments = []
        for stage in range(Nstages):
            op = operators[stage]
            for i in range(Ntasks):
                task_id = uuid4().int
                vm_ip = workers[(task_id) % len(workers)]
                
                ag_col = ""
                if stage == 0 and (stage + 1 < Nstages):
                    if operators[stage+1]["exe"] == "aggregate":
                        ag_col = operators[stage+1]["args"]

                task_info = {
                    "task_id": task_id,
                    "stage": stage + 1,
                    "vm": vm_ip,
                    "port": self.allocate_port_for_vm(vm_ip),
                    "operator": op,
                    "ag_column": ag_col,
                    "dest_filename": hydfs_dest_filename,
                    "pid": None,    # Will be updated later
                    "logfile": None # Will be updated later
                }
                task_assignments.append(task_info)

        self.tasks = task_assignments
        for t in task_assignments:
            self.send_start_task(t)

        # Start Source on Leader (Process now)
        stage0_tasks = [t for t in task_assignments if t["stage"] == 1]
        src_file = job["hydfs_src_directory"]
        input_rate = job["input_rate"]

        source = SourceProcess(src_file, stage0_tasks, input_rate, log_dir=".")
        source.start()
        self.log(f"[Leader] Source Process Started. PID: {source.pid}")

    def send_start_task(self, task):
        msg = {
            "command": "START_TASK",
            "task_id": task["task_id"],
            "stage": task["stage"],
            "port": task["port"],
            "operator": task["operator"],
            "ag_column": task["ag_column"],
            "dest_filename": task["dest_filename"]
        }
        vm = task["vm"]
        if task["stage"] == 1:
            stage2 = [t for t in self.tasks if t["stage"] == 2]
            msg["next_stage_tasks"] = [
                {"vm": t["vm"], "port": t["port"], "task_id": t["task_id"]}
                for t in stage2
            ]

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((vm, 9200)) 
            s.sendall(json.dumps(msg).encode())
            s.close()
        except Exception as e:
            self.log(f"[Leader] Failed to contact worker {vm}: {e}")

class RainstormWorker:
    def __init__(self, logfile, host="0.0.0.0", port=9200):
        self.host = host
        self.port = port
        self.logfile = logfile
        self.running_processes = {} # task_id -> Process Object
        self.leader_ip = None # Needs to know leader to report PID
        # Simple hack: we assume the one who connects to us first is Leader, or we find it via membership
        self.init_log()

    def init_log(self):
        open(self.logfile, "w").close()

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(self.logfile, "a") as f:
            f.write(f"[{timestamp}] {message}\n")

    def run(self):
        self.log(f"[Worker] Started on {self.host}:{self.port}")
        self.listen()

    def listen(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen(5)

        while True:
            conn, addr = server.accept()
            # Remember Leader IP roughly from who contacts us? 
            # Or hardcode in client like before. 
            # For reporting PID, we can assume Leader is at well-known location or pass it in START_TASK.
            # For MP4, let's assume Leader is always at fa25-cs425-9801 or pass it.
            
            t = threading.Thread(target=self.handle_message, args=(conn, addr), daemon=True)
            t.start()

    def handle_message(self, conn, addr):
        try:
            data = conn.recv(65535).decode("utf-8")
            if not data: return
            msg = json.loads(data)
            command = msg.get("command")

            if command == "START_TASK":
                self.handle_start_task(msg, addr[0]) # Pass leader IP candidate
                conn.sendall(json.dumps({"status": "OK"}).encode())
                
            elif command == "KILL_TASK":
                # Demo Command: Kill specific PID
                target_pid = msg.get("pid")
                self.handle_kill_task(target_pid)
                conn.sendall(json.dumps({"status": "OK"}).encode())
                
            else:
                self.log(f"Unknown command: {command}")
        except Exception as e:
            self.log(f"Error: {e}")
        finally:
            conn.close()

    def handle_kill_task(self, pid):
        self.log(f"[Demo] Received KILL_TASK for PID {pid}")
        try:
            os.kill(pid, signal.SIGKILL) # kill -9
            self.log(f"Successfully killed PID {pid}")
        except Exception as e:
            self.log(f"Failed to kill PID {pid}: {e}")

    def handle_start_task(self, task_info, leader_ip_hint):
        task_id = task_info.get("task_id")
        
        # If task already running (and alive), update routing only
        if task_id in self.running_processes:
            p = self.running_processes[task_id]
            if p.is_alive():
                self.log(f"Task {task_id} already running. (Updating routing not fully supported in multiprocess mode without IPC, but ignoring restart)")
                return
        
        # Start New Process
        # Note: Log files will be created in current dir "."
        t = TaskProcess(
            task_id, 
            task_info.get("operator"), 
            task_info.get("port"), 
            ".", 
            task_info.get("next_stage_tasks"), 
            task_info.get("ag_column"), 
            task_info.get("dest_filename")
        )
        t.start()
        self.running_processes[task_id] = t
        
        # Report PID back to Leader
        pid = t.pid
        logfile = t.log_file
        
        # Use leader_ip_hint or hardcode based on your cluster
        # Better: Leader sends its IP in START_TASK? 
        # For now, let's assume we reply to the sender (addr[0] from handle_message) or hardcode Leader Port 9100
        threading.Thread(target=self.report_pid_to_leader, args=(leader_ip_hint, 9100, task_id, pid, logfile)).start()

    def report_pid_to_leader(self, leader_ip, leader_port, task_id, pid, logfile):
        # We try leader_ip_hint. If you run leader elsewhere, this needs config.
        # But usually leader initiates connection, so leader_ip_hint is correct.
        msg = {
            "command": "UPDATE_PID",
            "task_id": task_id,
            "pid": pid,
            "logfile": logfile
        }
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((leader_ip, leader_port))
            s.sendall(json.dumps(msg).encode())
            s.close()
        except Exception as e:
            self.log(f"Failed to report PID to leader {leader_ip}: {e}")

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

if __name__ == "__main__":
    main()