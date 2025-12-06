import socket
import json
import sys

# Make sure this is the Leader's address
LEADER_IP = "fa25-cs425-9801.cs.illinois.edu"
LEADER_PORT = 9100
WORKER_PORT = 9200

def send_msg(ip, port, msg):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        s.sendall(json.dumps(msg).encode("utf-8"))
        
        # Receive response
        response = b""
        while True:
            chunk = s.recv(4096)
            if not chunk: break
            response += chunk
        s.close()
        return json.loads(response.decode())
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

def main():
    print("Looking for a victim (Stage 1 Filter task)...")
    
    # 1. Get task list
    tasks = send_msg(LEADER_IP, LEADER_PORT, {"command": "LIST_TASKS"})
    if not tasks:
        print("❌ No tasks running or Leader unreachable.")
        return

    # 2. Find target (Stage 1, Filter)
    target = None
    for t in tasks:
        # Check if it's a Filter operator
        if t.get("operator", {}).get("exe") == "filter":
            target = t
            break
    
    if target:
        vm = target['vm']
        pid = target['pid']
        tid = target['task_id']
        print(f"Target found: Task {tid} on {vm} (PID {pid})")
        
        # 3. Send kill command
        print(f"Sending KILL command...")
        resp = send_msg(vm, WORKER_PORT, {"command": "KILL_TASK", "pid": pid})
        print(f"Response: {resp}")
    else:
        print("No 'filter' task found to kill.")

if __name__ == "__main__":
    main()