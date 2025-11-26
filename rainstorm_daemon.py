# rainstorm_daemon.py
import argparse
import socket
import json
import time
import threading

class RainstormLeader:
    def __init__(self, logfile, host="0.0.0.0", port=9100):
        self.host = host
        self.port = port
        self.mp3_control_port = 9900
        self.logfile = logfile
        self.init_log()
        self.members = set()

    def init_log(self):
        open(self.logfile, "w").close()
        
    def run(self):
        threading.Thread(target=self.membership_check_loop, daemon=True).start()
        self.log(f"[Leader] Listening for job submissions on {self.host}:{self.port}")
        self.listen_for_jobs()

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        with open(self.logfile, "a") as f:
            f.write(message + "\n")
        print(f"[{timestamp}] {message}", flush=True)

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

    def listen_for_jobs(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()

        while True:
            conn, addr = srv.accept()
            data = conn.recv(65535)     # one job fits in one recv
            conn.close()

            try:
                job = json.loads(data.decode())
                self.log(f"[Leader] Received job: {job}")
            except Exception as e:
                print("[Leader] Invalid job received:", e)

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
