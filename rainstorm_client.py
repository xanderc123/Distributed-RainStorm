import argparse
import sys
import socket
import json

VALID_OPS = {"transform", "filter", "aggregate", "identity"}

LEADER_IP = "fa25-cs425-9801.cs.illinois.edu"
LEADER_PORT = 9100
WORKER_PORT = 9200

def parse_operator(op_exe, op_args_list):
    if op_exe not in VALID_OPS:
        print(f"Error: invalid operator '{op_exe}'.")
        sys.exit(1)
    
    # 修复 1: Identity 支持
    if op_exe == "identity":
        return {"exe": op_exe, "args": None}

    if op_exe == "aggregate":
        return {"exe": op_exe, "args": int(op_args_list[0])}
    
    if op_exe == "filter":
        return {"exe": op_exe, "args": op_args_list[0]}
    
    # 修复 2: Transform 支持单参数 (兼容 cut1-3)
    if op_exe == "transform":
        if len(op_args_list) == 1:
            if ' ' in op_args_list[0]:
                parts = op_args_list[0].split()
                return {"exe": op_exe, "args": (parts[0], parts[1])}
            else:
                return {"exe": op_exe, "args": op_args_list[0]}
        if len(op_args_list) >= 2:
            return {"exe": op_exe, "args": (op_args_list[0], op_args_list[1])}
            
    return None

def send_msg_to_leader(msg):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((LEADER_IP, LEADER_PORT))
        data = json.dumps(msg).encode("utf-8")
        s.sendall(data)
        reply_data = b""
        while True:
            chunk = s.recv(4096)
            if not chunk: break
            reply_data += chunk
        s.close()
        return json.loads(reply_data.decode())
    except Exception as e:
        print(f"Error connecting to Leader: {e}")
        return None

def send_msg_to_worker(vm_ip, msg):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((vm_ip, WORKER_PORT))
        s.sendall(json.dumps(msg).encode("utf-8"))
        reply = s.recv(1024).decode()
        s.close()
        return json.loads(reply)
    except Exception as e:
        print(f"Error connecting to Worker {vm_ip}: {e}")
        return None

def print_task_table(tasks):
    if not tasks:
        print("No tasks running.")
        return
    header = f"{'TASK_ID':<10} {'STAGE':<5} {'VM':<30} {'PID':<8} {'OP':<10} {'LOGFILE'}"
    print("\n" + "="*110)
    print(header)
    print("="*110)
    for t in tasks:
        tid = str(t['task_id'])[:8] + ".."
        vm = t['vm'].split('.')[0]
        pid = t.get('pid', 'N/A')
        op = t['operator']['exe']
        log = t.get('logfile', 'N/A')
        print(f"{tid:<10} {t['stage']:<5} {vm:<30} {pid:<8} {op:<10} {log}")
    print("="*110 + "\n")

def main():
    if len(sys.argv) < 2:
        sys.exit(0)

    cmd = sys.argv[1]

    if cmd == "list_tasks":
        resp = send_msg_to_leader({"command": "LIST_TASKS"})
        if resp: print_task_table(resp)

    elif cmd == "kill_task":
        if len(sys.argv) != 4:
            print("Usage: python3 rainstorm_client.py kill_task <vm> <pid>")
            sys.exit(1)
        send_msg_to_worker(sys.argv[2], {"command": "KILL_TASK", "pid": int(sys.argv[3])})
        print("Kill command sent.")

    else:
        try:
            int(cmd)
        except ValueError:
            print(f"Unknown command: {cmd}")
            sys.exit(1)

        parser = argparse.ArgumentParser()
        parser.add_argument("Nstages", type=int)
        parser.add_argument("Ntasks_per_stage", type=int)
        parser.add_argument("rest", nargs=argparse.REMAINDER)
        
        args = parser.parse_args(sys.argv[1:])

        operators = []
        rest = args.rest
        
        if not rest:
            sys.exit(1)

        op1_exe = rest[0]; rest = rest[1:]
        op1_args = []
        
        # Identity 处理逻辑
        if op1_exe == "identity":
             if rest and rest[0] == "": rest = rest[1:]
        elif op1_exe != "aggregate":
             op1_args.append(rest[0]); rest = rest[1:]
        else:
             op1_args.append(rest[0]); rest = rest[1:]
        operators.append(parse_operator(op1_exe, op1_args))

        if args.Nstages == 2:
            op2_exe = rest[0]; rest = rest[1:]
            op2_args = []
            if op2_exe == "identity":
                 if rest and rest[0] == "": rest = rest[1:]
            else:
                 op2_args.append(rest[0]); rest = rest[1:]
            operators.append(parse_operator(op2_exe, op2_args))

        hydfs_src = rest[0]
        hydfs_dest = rest[1]
        rest = rest[2:]
        
        exactly_once = False
        autoscale = False
        input_rate = 1000
        lw = None
        hw = None

        if len(rest) >= 3:
            exactly_once = rest[0].lower() == "true"
            autoscale = rest[1].lower() == "true"
            input_rate = int(rest[2])
        
        if len(rest) >= 5:
            lw = int(rest[3])
            hw = int(rest[4])
        
        job = {
            "command": "SUBMIT_JOB",
            "Nstages": args.Nstages,
            "Ntasks_per_stage": args.Ntasks_per_stage,
            "operators": operators,
            "hydfs_src_directory": hydfs_src,
            "hydfs_dest_filename": hydfs_dest,
            "exactly_once": exactly_once,
            "autoscale_enabled": autoscale,
            "input_rate": input_rate,
            "LW": lw, "HW": hw
        }
        print("Submitting Job...")
        send_msg_to_leader(job)

if __name__ == "__main__":
    main()