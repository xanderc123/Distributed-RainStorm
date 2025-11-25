import argparse
import socket
import threading
import time

from membership import Daemon

def main():
    with open("test.log", "w") as f:
        f.truncate(0)
    
    parser = argparse.ArgumentParser(description="Membership daemon for MP2")

    parser.add_argument("--port", type=int, required=True, help="UDP port to bind")
    parser.add_argument("--introducer", type=str, required=True, help="Introducer's IP:port-timestamp")
    parser.add_argument("--mode", choices=["gossip", "pingack"], default="gossip", help="Failure detection mode")
    parser.add_argument("--drop", type=float, default=0.0, help="Receiver-side message drop rate [0.0-1.0]")
    parser.add_argument("--t_fail", type=float, default=2, help="Time to suspect node after silence (seconds)")
    parser.add_argument("--t_cleanup", type=float, default=2, help="Time to delete node after suspicion (seconds)")
    parser.add_argument("--t_suspect", type=float, default=1.8, help="Suspicion hold time before deletion (seconds)")

    args = parser.parse_args()

    daemon = Daemon(
        hostname=socket.gethostname(),
        introducer=args.introducer,
        port=args.port,
        mode=args.mode,
        drop=args.drop,
        t_fail=args.t_fail,
        t_cleanup=args.t_cleanup,
        t_suspect=args.t_suspect,
        heartbeat_interval=1
    )

    if daemon.id != daemon.introducer:
        daemon.send_join()

    threading.Thread(target=daemon.receiver, daemon=True).start()
    threading.Thread(target=daemon.pingack_loop, daemon=True).start()
    threading.Thread(target=daemon.bandwidth_loop, daemon=True).start()
    threading.Thread(target=daemon.gossip, daemon=True).start()
    threading.Thread(target=daemon.failure_checker, daemon=True).start()
    threading.Thread(target=daemon.control_server, daemon=True).start()
    threading.Thread(target=daemon.replication_manager, daemon=True).start()
    threading.Thread(target=daemon.tcp_file_server, daemon=True).start()
    threading.Thread(target=daemon.tcp_file_server_verification, daemon=True).start()

    # time.sleep(60)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down server")
        
    print("\n=== Bandwidth Data ===")
    for timestamp, bw, mode in daemon.bandwidth_data:
        print(f"{timestamp},{bw},{mode}")

if __name__ == "__main__":
    main()

