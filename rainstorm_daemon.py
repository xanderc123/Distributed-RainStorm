import argparse
import socket
import json
import time
import threading
import os
import signal
from uuid import uuid4
from tasks import SourceProcess, TaskProcess
import multiprocessing

class RainstormLeader:
    def __init__(self, logfile, host="0.0.0.0", port=9100):
        self.host = host
        self.port = port
        self.logfile = logfile
        self.init_log()
        self.members = set()
        self.tasks = [] 
        self.vm_next_port = {}
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.port))
        self.lock = threading.Lock() # Protect task list
       
        self.manager = multiprocessing.Manager()
        self.source_tasks = None # 专门给 Source 用的共享列表

    def init_log(self):
        open(self.logfile, "w").close()
        self.log("[Leader] RainStorm System Started")

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(self.logfile, "a") as f:
            f.write(f"[{timestamp}] {message}\n")
        print(f"[{timestamp}] {message}", flush=True)

    def allocate_port_for_vm(self, vm_ip):
        base = 10000
        limit = 10050
        if vm_ip not in self.vm_next_port:
            self.vm_next_port[vm_ip] = base
            return base
        next_port = self.vm_next_port[vm_ip] + 1
        self.vm_next_port[vm_ip] = next_port
        return next_port

    def listen(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_connection, args=(conn,)).start()

    def handle_connection(self, conn):
        try:
            data = conn.recv(65535).decode("utf-8")
            if not data: return
            msg = json.loads(data)
            cmd = msg.get("command")

            if cmd == "LIST_TASKS":
                conn.sendall(json.dumps(self.tasks).encode())
            elif cmd == "SUBMIT_JOB":
                self.handle_job_submission(msg)
                conn.sendall(json.dumps("OK").encode())
            elif cmd == "UPDATE_PID":
                self.handle_update_pid(msg)
                conn.sendall(json.dumps("OK").encode())
            elif cmd == "TASK_FAILED": # <-- New: Handle failure report
                self.handle_task_failure(msg)
                conn.sendall(json.dumps("OK").encode())
            elif cmd == "UPDATE_RATES":
                self.handle_rate_updates(msg.get("updates"))

        except Exception as e:
            print(f"Error: {e}")
        finally:
            conn.close()
   
    # 新增处理函数
    def handle_rate_updates(self, updates):
        # Demo 要求: "Every second, log the tuples/sec processing rate at each task"
        # 我们可以直接打印，或者存起来每秒统一打。为了简单，收到就打，或者在这里更新内存。
        timestamp = time.strftime("%H:%M:%S")
        for u in updates:
            tid = u["task_id"]
            rate = u["rate"]
            # 找到 Task 的更多信息以便打印 (如 VM)
            vm = "Unknown"
            with self.lock:
                for t in self.tasks:
                    if t["task_id"] == tid:
                        vm = t["vm"]
                        # 更新任务的当前速率 (为 AutoScaling 做准备)
                        t["current_rate"] = rate 
                        break
            
            # 打印日志 [Demo Requirement]
            # 格式: [RateLog] Task <ID> on <VM>: <Rate> tuples/sec
            self.log(f"[RateLog] Task {str(tid)[:8]}.. on {vm}: {rate:.2f} tuples/sec")

    def handle_update_pid(self, msg):
        with self.lock:
            for t in self.tasks:
                if t["task_id"] == msg["task_id"]:
                    t["pid"] = msg["pid"]
                    t["logfile"] = msg["logfile"]
                    self.log(f"[TaskStarted] Task {t['task_id']} on {t['vm']} PID: {t['pid']}")
                    break

    def handle_task_failure(self, msg):
        failed_task_id = msg.get("task_id")
        self.log(f"[Failure] Received failure report for Task {failed_task_id}")
        
        with self.lock:
            # Find the task
            target_task = None
            for t in self.tasks:
                if t["task_id"] == failed_task_id:
                    target_task = t
                    break
            
            if not target_task: return

            # Reschedule Logic
            old_vm = target_task["vm"]
            
            # Simple Strategy: Restart on the SAME VM first (if node is alive), 
            # or move to another. For Demo "Kill Task", node is alive, so restart on same VM is fastest.
            # But let's rotate to prove we can move it.
            workers = list(self.members)
            if not workers: return
            
            # Pick a new VM (Round Robin)
            new_vm = workers[(target_task["task_id"] + 1) % len(workers)]
            new_port = self.allocate_port_for_vm(new_vm)
            
            self.log(f"[Recovery] Restarting Task {failed_task_id}: {old_vm} -> {new_vm}")
            
            target_task["vm"] = new_vm
            target_task["port"] = new_port
            target_task["pid"] = None # Reset
            target_task["failed_task_log_id"] = failed_task_id
            
            self.send_start_task(target_task)
            
            # Important: Update routing for upstream tasks
            self.update_routing_tables()
            if target_task["stage"] == 1:
                for i, t in enumerate(self.source_tasks):
                    if t["task_id"] == target_task["task_id"]:
                        self.source_tasks[i] = target_task # 更新共享内存
                        self.log(f"[Leader] Updated Source routing for Task {t['task_id']}")
                        break

    def update_routing_tables(self):
        # Resend start command to all tasks to update their routing maps
        for t in self.tasks:
            self.send_start_task(t)

    def retrieve_alive_members(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2)
            s.connect((self.host, 9900))
            s.sendall(b"LIST_MEM\n")
            data = s.recv(4096).decode("utf-8").strip()
            s.close()
            return [n.split(":")[0] for n in data.split() if "fa25-cs425" in n]
        except: return []

    def membership_check_loop(self):
        while True:
            new_members = set(self.retrieve_alive_members())
            if new_members != self.members:
                self.members = new_members
                # (Optional: Handle node failure here too)
            time.sleep(2)

    def handle_job_submission(self, job):
        # (Same as before)
        Nstages = job["Nstages"]
        Ntasks = job["Ntasks_per_stage"]
        operators = job["operators"]
        hydfs_dest = job.get("hydfs_dest_filename")
        src_file = job["hydfs_src_directory"]
        input_rate = job["input_rate"]

        workers = list(self.members)
        if not workers: return

        self.autoscale_enabled = job.get("autoscale_enabled", False)
        self.LW = job.get("LW")
        self.HW = job.get("HW")
        self.cooldown_timer = {} # 记录每个 Stage 上次伸缩的时间 {stage_num: timestamp}

        new_tasks = []
        for stage in range(Nstages):
            op = operators[stage]
            for i in range(Ntasks):
                tid = uuid4().int
                vm = workers[tid % len(workers)]
                ag_col = ""
                if stage == 0 and (stage+1 < Nstages) and operators[stage+1]["exe"] == "aggregate":
                    ag_col = operators[stage+1]["args"]
                
                new_tasks.append({
                    "task_id": tid, "stage": stage+1, "vm": vm, 
                    "port": self.allocate_port_for_vm(vm), "operator": op,
                    "ag_column": ag_col, "dest_filename": hydfs_dest,
                    "pid": None, "logfile": None
                })
        
        with self.lock:
            self.tasks = new_tasks
            for t in self.tasks: self.send_start_task(t)
        
     # --- 关键修改开始：使用 Shared List ---
        # 1. 提取 Stage 1 任务
        st0 = [t for t in new_tasks if t["stage"]==1]
        
        # 2. 转换为 Manager 管理的共享列表 (这样 Leader 修改它时，Source 也能看到)
        self.source_tasks = self.manager.list(st0) 
        
        # 3. 启动 Source，传入这个共享列表
        SourceProcess(src_file, self.source_tasks, input_rate, ".").start()
        # --- 关键修改结束 ---

        # 日志要保留，这是 Demo 要求的
        self.log("[Leader] Job started.")
        self.log("[Leader] Job started with Autoscale=" + str(self.autoscale_enabled))

    def send_start_task(self, task):
        msg = {
            "command": "START_TASK", "task_id": task["task_id"], "stage": task["stage"],
            "port": task["port"], "operator": task["operator"], 
            "ag_column": task["ag_column"], "dest_filename": task["dest_filename"],
            "failed_task_log_id": task.get("failed_task_log_id", "")
        }
        if task["stage"] == 1:
            st2 = [t for t in self.tasks if t["stage"]==2]
            msg["next_stage_tasks"] = [{"vm": t["vm"], "port": t["port"], "task_id": t["task_id"]} for t in st2]
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((task["vm"], 9200))
            s.sendall(json.dumps(msg).encode())
            s.close()
        except Exception as e:
            self.log(f"Failed to start task on {task['vm']}: {e}")

    def autoscale_monitor_loop(self):
        while True:
            time.sleep(2.0) # 每2秒检查一次
            
            if not getattr(self, "autoscale_enabled", False):
                continue
            
            # 1. 计算每个 Stage 的平均速率
            stage_rates = {} # {stage: [rate1, rate2, ...]}
            
            with self.lock:
                for t in self.tasks:
                    s = t["stage"]
                    r = t.get("current_rate", 0)
                    if s not in stage_rates: stage_rates[s] = []
                    stage_rates[s].append(r)

            # 2. 检查每个 Stage 是否需要伸缩
            for stage, rates in stage_rates.items():
                if not rates: continue
                
                # 只有 Stage 2 (或非 Source Stage) 需要伸缩
                # Source (Stage 0/Internal) 不归我们管，Stage 1 也可以伸缩
                
                avg_rate = sum(rates) / len(rates)
                
                # 检查冷却时间 (假设冷却 5 秒)
                last_scale = self.cooldown_timer.get(stage, 0)
                if time.time() - last_scale < 5:
                    continue

                # 判定逻辑
                if self.HW and avg_rate > self.HW:
                    self.log(f"[Autoscale] Stage {stage} AvgRate {avg_rate:.2f} > HW {self.HW}. Scaling UP.")
                    self.scale_up_stage(stage)
                    self.cooldown_timer[stage] = time.time()
                    
                elif self.LW and avg_rate < self.LW:
                    # 只有当 Task 数大于 1 时才缩容
                    if len(rates) > 1:
                        self.log(f"[Autoscale] Stage {stage} AvgRate {avg_rate:.2f} < LW {self.LW}. Scaling DOWN.")
                        self.scale_down_stage(stage)
                        self.cooldown_timer[stage] = time.time()

    def scale_up_stage(self, stage):
        """增加一个 Task 到指定 Stage"""
        with self.lock:
            # 1. 复制该 Stage 的 Operator 配置 (找一个现成的 Task)
            template_task = None
            for t in self.tasks:
                if t["stage"] == stage:
                    template_task = t
                    break
            if not template_task: return

            # 2. 创建新 Task
            workers = list(self.members)
            if not workers: return
            
            new_tid = uuid4().int
            # 简单的负载均衡: 随机选一个节点
            target_vm = workers[new_tid % len(workers)]
            
            new_task = {
                "task_id": new_tid,
                "stage": stage,
                "vm": target_vm,
                "port": self.allocate_port_for_vm(target_vm),
                "operator": template_task["operator"],
                "ag_column": template_task["ag_column"],
                "dest_filename": template_task["dest_filename"],
                "pid": None, "logfile": None, "current_rate": 0
            }
            
            self.tasks.append(new_task)
            self.log(f"[ScaleUp] Started new Task {new_tid} on {target_vm}")
            
            # 3. 启动新任务
            self.send_start_task(new_task)
            
            # 4. 更新上游路由 (让 Stage N-1 知道这个新 Task 的存在)
            # 我们直接全量更新所有任务路由，最简单安全
            self.update_routing_tables()
            
    def scale_down_stage(self, stage):
        victim = None
        with self.lock:
            candidates = [t for t in self.tasks if t["stage"] == stage]
            if len(candidates) <= 1: return
            victim = candidates[-1] # 牺牲者
            
            # 从列表中移除
            self.tasks.remove(victim)

        if victim and victim.get("pid"):
            self.log(f"[ScaleDown] Removing Task {victim['task_id']} PID {victim['pid']}")
            # 通知 Worker 杀掉进程
            self.send_kill_command_by_pid(victim["vm"], victim["pid"])
        
        # 更新路由
        self.update_routing_tables()

    def send_kill_command_by_pid(self, vm, pid):
        try:
            msg = {"command": "KILL_TASK", "pid": pid}
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((vm, 9200))
            s.sendall(json.dumps(msg).encode())
            s.close()
        except Exception as e:
            self.log(f"Failed to send KILL to {vm}: {e}")

    def run(self):
        threading.Thread(target=self.membership_check_loop, daemon=True).start()
        threading.Thread(target=self.autoscale_monitor_loop, daemon=True).start()
        self.log(f"[Leader] Listening on {self.port}")
        self.listen()


class RainstormWorker:
    def __init__(self, logfile, host="0.0.0.0", port=9200):
        self.host = host
        self.port = port
        self.logfile = logfile
        self.running_processes = {} # task_id -> Process
        self.init_log()
        # Assumed Leader IP for reporting failures (In demo, leader is fixed)
        self.leader_ip = "fa25-cs425-9801.cs.illinois.edu" 

    def init_log(self):
        open(self.logfile, "w").close()

    def log(self, msg):
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with open(self.logfile, "a") as f: f.write(f"[{ts}] {msg}\n")

    def run(self):
        self.log(f"[Worker] Listening on {self.port}")
        # Start Monitor Thread
        threading.Thread(target=self.monitor_processes, daemon=True).start()
        self.listen()

    def listen(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_message, args=(conn, addr)).start()

    def handle_message(self, conn, addr):
        try:
            data = conn.recv(65535).decode()
            if not data: return
            msg = json.loads(data)
            cmd = msg.get("command")
            
            if cmd == "START_TASK":
                self.handle_start_task(msg)
                conn.sendall(json.dumps({"status": "OK"}).encode())
            elif cmd == "KILL_TASK":
                self.handle_kill_task(msg.get("pid"))
                conn.sendall(json.dumps({"status": "OK"}).encode())
        finally:
            conn.close()

    def handle_kill_task(self, pid):
        self.log(f"[Demo] Killing PID {pid}")
        try: os.kill(pid, signal.SIGKILL)
        except Exception as e: self.log(f"Kill failed: {e}")

    def handle_start_task(self, info):
        tid = info["task_id"]
        
        # --- 优化：防止重复启动 ---
        if tid in self.running_processes:
            entry = self.running_processes[tid]
            # 如果进程还活着，或者刚启动(counter存在)，就只更新路由
            if entry.get("process") and entry["process"].is_alive():
                self.log(f"[Info] Task {tid} already running. Updating routing only.")
                entry["process"].next_stage_tasks = info.get("next_stage_tasks")
                return
        # 创建共享计数器 (类型 'i' 为整数, 初始值 0)
        import multiprocessing
        counter = multiprocessing.Value('i', 0)

        t = TaskProcess(
            tid, info["operator"], info["port"], ".", 
            info.get("next_stage_tasks"), info.get("ag_column"), info.get("dest_filename"),
            shared_counter=counter,
            failed_task_log_id=info.get("failed_task_log_id") # <--- 必须加上这个！
        )
        t.start()
        
        # 保存 Process 和 Counter，以及上一次的统计值
        self.running_processes[tid] = {
            "process": t,
            "counter": counter,
            "last_count": 0,
            "last_time": time.time()
        }
        
        threading.Thread(target=self.report_pid, args=(tid, t.pid, t.log_file)).start()

    def report_pid(self, tid, pid, logfile):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.leader_ip, 9100))
            s.sendall(json.dumps({"command": "UPDATE_PID", "task_id": tid, "pid": pid, "logfile": logfile}).encode())
            s.close()
        except: pass

    def monitor_processes(self):
        while True:
            time.sleep(1.0) # Demo 要求每秒记录
            
            # 收集本节点所有任务的速率
            updates = []
            
            for tid, info in list(self.running_processes.items()):
                proc = info["process"]
                if not proc.is_alive():
                    self.log(f"[Monitor] Task {tid} died.")
                    del self.running_processes[tid]
                    self.report_failure(tid)
                    continue
                
                # 计算速率
                with info["counter"].get_lock():
                    curr_val = info["counter"].value
                
                now = time.time()
                delta_count = curr_val - info["last_count"]
                delta_time = now - info["last_time"]
                
                rate = 0.0
                if delta_time > 0:
                    rate = delta_count / delta_time
                
                # 更新历史
                info["last_count"] = curr_val
                info["last_time"] = now
                
                updates.append({"task_id": tid, "rate": rate})
            
            # 批量发送给 Leader
            if updates:
                self.send_rates_to_leader(updates)

    def send_rates_to_leader(self, updates):
        try:
            msg = {"command": "UPDATE_RATES", "updates": updates}
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.leader_ip, 9100))
            s.sendall(json.dumps(msg).encode())
            s.close()
        except: pass

    def report_failure(self, tid):
        self.log(f"[Monitor] Reporting failure of Task {tid} to Leader")
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.leader_ip, 9100))
            s.sendall(json.dumps({"command": "TASK_FAILED", "task_id": tid}).encode())
            s.close()
        except Exception as e:
            self.log(f"Failed to report failure: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    parser.add_argument("--logfile", type=str)
    args = parser.parse_args()
    if args.mode == "leader": RainstormLeader(args.logfile).run()
    if args.mode == "worker": RainstormWorker(args.logfile).run()

if __name__ == "__main__":
    main()
