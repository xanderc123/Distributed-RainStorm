import argparse
import bisect
from collections import deque
import hashlib
import json
import random
import socket
import struct
import threading
import time
from typing import Dict
from suspicion import start_suspect, clear_suspect, delete_member
import pathlib
import os
import shutil

from hydfs_client import recv_msg

def get_hash(name):
        h = int(hashlib.sha1(name.encode('utf-8')).hexdigest(), 16)
        return (h % 4096) + 1

class Member:
    def __init__(self, id, heartbeat, last_heard, status, incarnation):
        self.id = id #node id
        self.heartbeat = heartbeat #for gossip specifically
        self.last_heard = last_heard if last_heard is not None else time.monotonic()
        self.status = status # "alive", "suspect", "failed", "left"
        self.incarnation = incarnation 
        self.suspect_deadline = None 
        self.cleanup_timeout = None #for gossip
        self.node_id = get_hash(self.id)

class Daemon:
    def __init__(self, hostname, introducer, port, mode, drop,
                 t_fail, t_cleanup, t_suspect, heartbeat_interval):
        self.port = port
        self.hostname = hostname 
        self.startup_timestamp = int(time.monotonic())
        self.id = f"{hostname}:{port}:{self.startup_timestamp}"
        self.mode = mode
        self.drop = drop
        self.suspicion_enabled = False 
        self.introducer = introducer #  in hostname:port 
        self.heartbeat_interval = heartbeat_interval
        self.t_fail = t_fail
        self.t_cleanup = t_cleanup
        self.t_suspect = t_suspect
        self.members: Dict[str, Member] = {}
        if self.id not in self.members:
            self.members[self.id] = Member(
                id=self.id,
                heartbeat=0,
                last_heard=time.monotonic(),
                status="alive",
                incarnation=int(time.monotonic()),
            )

        self.piggy = deque(maxlen=64)
        self.lock = threading.Lock()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.setblocking(True)
        self.bandwidth_data = []
        self.pending = {}  # for nonce (member_id, deadline)
        self.bytes_out = 0
        self.last_bw_log = time.monotonic()
        self.control_port = 9900
        self.t_ping = 0.30

        # mp3_file_system
        self.file_system_port = 9002
        # file_replica_verification_port
        self.file_replica_verification_port = 9003
        # 2. Define the file storage path (e.g.: ./files_9000/)
        self.file_storage_dir = pathlib.Path(f"DFS")
        # Clear storage directory (if restarting)
        self.clean_storage_on_startup()
        # Create this directory (if it doesn't exist yet)
        os.makedirs(self.file_storage_dir, exist_ok=True)
        # 3. Set the replication factor (n=3)
        self.replication_factor = 3
        # 4. For rebalancing thread to detect membership changes
        self.previous_members = set()
        # mangage the lock for hydfs file
        self.global_file_lock = threading.Lock()
        self.file_locks: Dict[str, threading.Lock] = {}

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_message=f"[{timestamp}] {message}"
        # write local log
        with open("test.log", "a") as f:
            f.write(log_message + "\n")
        
        if ("ERROR" in message or "SUSPECT" in message or "FAILED" in message):
            print(log_message, flush=True)

    def broadcast_delete(self, target_id: str):
        msg = {
            "type": "DELETE",
            "id": target_id,
            "target": target_id,
            "from": self.id,  
        }
        data = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        for m in list(self.members.values()):
            if m.id == self.id or m.status != "alive":
                continue
            try:
                ip, port = self.parse_id_for_sending(m.id)
                if not ip: continue
                self.sendJSON(msg, (ip, port))
            except Exception as e:
                self.log(f"broadcast_delete: failed to {m.id}: {e}")

    def update_members(self, incoming: dict):
        node_id = incoming.get("id")
        incomingStatus = incoming.get("status")
        incomingIncarnation = incoming.get("incarnation")
        
        if node_id is None or incomingStatus is None or incomingIncarnation is None:
            self.log(f"Skipped update for {incoming} (missing keys)")
            return
        
        current = self.members.get(node_id)

        if current and current.status == "left" and incomingIncarnation <= current.incarnation:
            self.log(f"Ignoring update from {node_id} with status=left and stale/incarnation={incomingIncarnation}")
            return

        if current is None:
            self.members[node_id] = Member(
                id=node_id,
                heartbeat=0,
                last_heard=time.monotonic(),
                status=incomingStatus,
                incarnation=incomingIncarnation,
            )
            self.log(f"Added new member {node_id} with status {incomingStatus}")
            return 
        
        if incomingStatus == "left":
            if current.status != "left" or incomingIncarnation > current.incarnation:
                current.status = "left"
                current.last_heard = time.monotonic()
                current.cleanup_timeout = time.monotonic() + self.t_cleanup
                current.suspect_deadline = None
                current.incarnation = incomingIncarnation 
                self.log(f"Member {node_id} marked as LEFT")
            else:
                self.log(f"Ignored stale leave for {node_id} (incomingInc={incomingIncarnation}, currentInc={current.incarnation})")
            return

            
        if incomingStatus == "failed":
            if current.status != "failed":
                current.status = "failed"
                current.last_heard = time.monotonic()
                current.cleanup_timeout = time.monotonic() + self.t_cleanup
                current.suspect_deadline = None
                self.log(f"Member {node_id} marked as FAILED (via gossip)")
            return


        if current.status == "failed" and incomingIncarnation <= current.incarnation:
            self.log(f"Ignoring stale update from {node_id} (incarnation={incomingIncarnation}) — already failed with {current.incarnation}")
            return

        if self.suspicion_enabled:
            if incomingIncarnation > current.incarnation:
                old_status = current.status
                clear_suspect(self.members, node_id)
                current.incarnation = incomingIncarnation
                current.last_heard = time.monotonic()
                current.status = incomingStatus
                self.log(f"Updated member {node_id}: incarnation increased to {incomingIncarnation}, status {old_status} -> {incomingStatus}")

            elif current.incarnation == incomingIncarnation:
                if current.status == "alive" and incomingStatus == "suspect":
                    current.status = "suspect" 
                    current.suspect_deadline = time.monotonic() + self.t_suspect
                    self.log(f"Member {node_id} marked as suspect")

                elif current.status == "suspect" and incomingStatus == "alive":
                    clear_suspect(self.members, node_id)
                    current.status = "alive"
                    current.suspect_deadline = None
                    self.log(f"Member {node_id} status cleared to alive")   
        else:
            if incomingIncarnation > current.incarnation: #other process has a higher incanaitn so trust that
                old_status = current.status
                current.status = incomingStatus
                current.incarnation = incomingIncarnation
                current.last_heard = time.monotonic()
                self.log(f"Updated member {node_id}: status {old_status} -> {incomingStatus} (inc={incomingIncarnation})")

            elif incomingIncarnation == current.incarnation: #no update in incarnation
                if current.status != incomingStatus: #check if status was updates
                    old_status = current.status
                    current.status = incomingStatus
                    current.last_heard = time.monotonic()
                    self.log(f"Updated member {node_id}: status {old_status} -> {incomingStatus}")
            else:
                current.last_heard = time.monotonic()

    def sendJSON(self, obj: dict, addr_tuple) -> int:
        data = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        sent = self.sock.sendto(data, addr_tuple)
        self.bytes_out += sent
        return sent
    
    def apply_piggy(self, p, now=None):

        if now is None:
            now = time.monotonic()

        kind = p.get("type")
        target = p.get("target") or p.get("id")  

        if not kind or not target or target == self.id:
            return

        if kind == "JOINNOTICE":
            self.updateInsertAlive(target)
            return

        if kind == "SUSPECT" and self.suspicion_enabled:
            start_suspect(self.members, target, now, self.t_suspect)
            return

        if kind == "DELETE":
            delete_member(self.members, target, reason="piggy")
            if self.mode == "pingack":
                if self.members.pop(target, None) is not None:
                    self.log(f"Purged {target} immediately (pingack, piggyback DELETE)")
            else:
                mm = self.members.get(target)
                if mm is not None:
                    if mm.cleanup_timeout is None:  
                        mm.cleanup_timeout = now + self.t_cleanup
                        self.log(f"Set cleanup_timeout for {target} to {mm.cleanup_timeout:.2f} (now {now:.2f})")

            return

    def failure_checker(self):
        
        while True:
            time.sleep(0.1)
            now = time.monotonic()

            with self.lock:
                if self.members.get(self.id) and self.members[self.id].status == "left":
                    continue
                if self.mode == "pingack": #for pinkg ack only
                    for nonce, (mid, deadline) in list(self.pending.items()):
                        if now <= deadline:
                            continue

                        self.pending.pop(nonce, None)

                        if not mid or mid == self.id:
                            continue
                        m = self.members.get(mid)
                        if m is None:
                            continue  

                        self.log(f"Pending ping timeout for {mid}, nonce {nonce}, deadline {deadline:.2f}")

                        if self.suspicion_enabled:
                            # Ssuspects it then delete on suspect_deadline
                            if start_suspect(self.members, mid, now, self.t_suspect):
                                self.log(f"EVENT=SUSPECT target={mid} by=self reason=ping_timeout")
                                self.piggy.append({"type": "SUSPECT", "target": mid, "id": mid, "status": "suspect","incarnation": self.members[mid].incarnation})
                        else:
                            # mark failed and deletes from mem list
                            delete_member(self.members, mid, reason="no_suspect")
                            self.log(f"EVENT=DELETE target={mid} reason=no_suspect")
                            inc = self.members.get(mid).incarnation if mid in self.members else 0
                            self.piggy.append({"type": "DELETE", "target": mid, "id": mid, "status": "suspect", "incarnation": inc})
                            self.members.pop(mid, None) 
                            self.broadcast_delete(mid)

                    if self.pending:
                        self.pending = {
                            n: (mid, dl) for n, (mid, dl) in self.pending.items()
                            if mid in self.members
                        }

                if self.mode == "gossip": #for gossip only
                    for mid, m in list(self.members.items()):
                        if mid == self.id or m.status != "alive":
                            continue
                        if (now - m.last_heard) <= self.t_fail:
                            continue

                        if self.suspicion_enabled: #starts sus if enableed 
                            if start_suspect(self.members, mid, now, self.t_suspect):
                                self.log(f"EVENT=SUSPECT target={mid} by=self reason=quiet")
                                self.piggy.append({"type": "SUSPECT", "target": mid, "id": mid, "status": "suspect","incarnation": self.members[mid].incarnation})
                        else:
                            delete_member(self.members, mid, reason="no_suspect") # mark failed and deletes after cleanup window
                            self.log(f"EVENT=DELETE target={mid} reason=no_suspect")
                            self.piggy.append({"type": "DELETE", "target": mid, "id": mid, "status": "suspect","incarnation": self.members[mid].incarnation})
                            mm = self.members.get(mid)
                            if mm is not None:
                                mm.cleanup_timeout = now + self.t_cleanup
                            self.broadcast_delete(mid)

                if self.suspicion_enabled: #for both modes when sus is enableed
                    for mid, m in list(self.members.items()):
                        if mid == self.id or m.status != "suspect":
                            continue
                        if not m.suspect_deadline or now <= m.suspect_deadline:
                            continue

                        delete_member(self.members, mid, reason="timeout")
                        self.log(f"EVENT=DELETE target={mid} reason=timeout")
                        self.piggy.append({"type": "DELETE", "target": mid, "id": mid, "status": "suspect","incarnation": self.members[mid].incarnation})

                        if self.mode == "pingack":
                            self.members.pop(mid, None)  # immediate deletes
                        else:
                            mm = self.members.get(mid)
                            if mm is not None:
                                mm.cleanup_timeout = now + self.t_cleanup
                        
                        self.broadcast_delete(mid)

                for mid, m in list(self.members.items()): #deletes after cleanup timeout for gossip
                    if m.cleanup_timeout and now > m.cleanup_timeout:
                        self.log(f"Member {mid} removed after cleanup timeout")
                        self.members.pop(mid, None)

                if self.mode == "pingack": # deletes failed for pingack
                    for mid, m in list(self.members.items()):
                        if mid != self.id and m.status == "failed":
                            self.log(f"Purging failed member immediately (pingack): {mid}")
                            self.members.pop(mid, None)

    def random_alive_peers(self, peers):
        alive_members = []
        for m in self.members.values():
            if m.status == "alive" and m.id != self.id:
                alive_members.append(m)
        return random.sample(alive_members, k=min(peers, len(alive_members)))

    def send_heartbeat(self, member, piggyback_data):
        if not member:
            return
        if self.members[self.id].status == "left":
            return  
        ip, port = self.parse_id_for_sending(member.id)
        if not ip:
            self.log(f"Invalid member id format for {member.id}")
            return
                
        message = {
            "type": "HEARTBEAT",
            "id": self.id,
            "incarnation": self.members[self.id].incarnation,
            "status": "alive",
            "piggyback": piggyback_data,
        }
        data = json.dumps(message).encode("utf-8")

        self.sendJSON(message, (ip, port))
    
    def gossip(self):
        while True:
            time.sleep(self.heartbeat_interval)
            if (self.mode != "gossip"):
                continue
            with self.lock:
                selected_members = self.random_alive_peers(2)
                piggyback_data = list(self.piggy)
                for selected in selected_members:
                    self.send_heartbeat(selected, piggyback_data)
    
    def pingack_loop(self):
        while True:
            time.sleep(self.t_ping)
            if self.mode != "pingack":
                continue

            peers = self.random_alive_peers(1)
            if not peers:
                continue

            ip, port = self.parse_id_for_sending(peers[0].id)
            if not ip: continue
            mid = peers[0].id

            nonce = random.randint(1, 2**31 - 1)
            self.pending[nonce] = (mid, time.monotonic() + self.t_fail)

            pkt = {
                "type": "PING",
                "from": self.id,
                "nonce": nonce,
                "piggyback": list(self.piggy)
            }
            self.sendJSON(pkt, (ip, port))

    def send_join(self): #non introducer asks introducer to join
    
        join_message = {
            "type": "JOIN",
            "id": self.id,
            "incarnation": self.members[self.id].incarnation,
            "status": "alive",
        }

        data = json.dumps(join_message).encode("utf-8")

        try:
            ip = self.introducer.split(":")[0]
            port = int(self.introducer.split(":")[1])
            self.sendJSON(join_message, (ip, port))
            print(f"Sent join request to introducer {self.introducer}")
        except Exception as e:
            print(f"Failed to send join request to introducer {self.introducer}: {e}")

    def updateInsertAlive(self, member_id: str):
        if member_id not in self.members:
            self.members[member_id] = Member(
                id=member_id,
                heartbeat=0,
                last_heard=time.monotonic(),
                status="alive",
                incarnation=0
            )
            return

        m = self.members[member_id]
        if m.status != "left":
            m.status = "alive"
            m.last_heard = time.monotonic()
            m.suspect_deadline = None

    def handle_msg(self, message, addr = None): #handles diff incoming messafes
        msg_type = message.get("type")
        src = message.get("from")
        if src:
            self.updateInsertAlive(src)

        piggy = message.get("piggyback", [])
        now = time.monotonic()
        for p in piggy:
            self.apply_piggy(p, now)

        if msg_type == "JOIN":
            self.handle_join(message)
        elif msg_type == "HEARTBEAT":
            self.handle_heartbeat(message) 
        elif msg_type == "PING":
            self.handle_ping(message, addr)
        elif msg_type == "ACK":
            self.handle_ack(message)
        elif msg_type == "SUSPECT":
            self.handle_suspect(message)
        elif msg_type == "DELETE":
            self.handle_delete(message)
        elif msg_type == "LEAVE":
            self.handle_leave(message)
        elif msg_type == "WELCOME":
            self.handle_welcome(message)
        else:
            self.log(f"unknown message type: {msg_type}")

    def handle_join(self, message):
        if not self.id.startswith(self.introducer):
            self.log(f"Ignored JOIN request because this node is not the introducer")
            return
        node_id = message.get("id")
        self.log(f"Received JOIN request from {node_id}")

        if node_id in self.members:
            print(f"Member {node_id} already exists. Ignoring join request.")
            return
        
        self.members[node_id] = Member(
            id=node_id,
            heartbeat=0,
            last_heard=time.monotonic(),
            status="alive",
            incarnation=message.get("incarnation", 0)
        )

        self.piggy.append({"type": "JOINNOTICE", "id": node_id})

        self.log(f"Added member {node_id} to membership list")

        piggy = list(self.piggy)

        welcome_message = {
            "type": "WELCOME",
            "from": self.id,
            "memberships": [{ 
                "id": m.id,
                "heartbeat": m.heartbeat,
                "status": m.status,
                "incarnation": m.incarnation
            } for m in self.members.values()],
            "piggyback": piggy
        }

        data = json.dumps(welcome_message).encode("utf-8")

        try:
            ip, port = self.parse_id_for_sending(node_id)
            if not ip: return
            self.sendJSON(welcome_message, (ip, port))
            print(f'Sent welcome message to {node_id}')
        except Exception as e:
            print(f'Failed to send welcome message to {node_id}: {e}')

        notice_piggy = [{"type": "JOINNOTICE", "id": node_id}]
        for m in list(self.members.values()):
            if m.id in (self.id, node_id):   # skip self and the newcomer
                continue
            if m.status != "alive":
                continue
            try:
                ip, port_timestamp = m.id.split(":")
                port = int(port_timestamp.split("-")[0])
                hb = {
                    "type": "HEARTBEAT",
                    "id": self.id,
                    "incarnation": self.members[self.id].incarnation,
                    "status": "alive",
                    "piggyback": notice_piggy
                }
                self.sendJSON(hb, (ip, port))
                self.log(f"Notified {m.id} of JOIN {node_id}")
            except Exception as e:
                self.log(f"Failed to notify {m.id} of JOIN {node_id}: {e}")

    def handle_heartbeat(self, message):
        node_id = message.get("id")
        update = {
            "id": node_id,
            "incarnation": message.get("incarnation"),
            "status": message.get("status")
        }
        with self.lock:
            self.update_members(update)
            if node_id in self.members and self.members[node_id].status != "left":
                self.members[node_id].last_heard = time.monotonic()
                self.log(f"Heartbeat received and updated from {node_id}")

            piggyback = message.get("piggyback", [])
            now = time.monotonic()
            for p in piggyback:
                self.apply_piggy(p, now)

    def handle_welcome(self, message):
        self.log(f"Received WELCOME message from introducer {message.get('from')}")
        memberships = message.get("memberships", [])
        for entry in memberships:
            self.update_members(entry)
        piggyback = message.get("piggyback", [])
        now = time.monotonic()
        for p in piggyback:
            self.apply_piggy(p, now)


    def handle_ping(self, message, addr):
        nonce = message.get("nonce")
        ack = {
            "type": "ACK",
            "from": self.id,
            "nonce": nonce,
            "piggyback": list(self.piggy)
            }
        self.sendJSON(ack, addr)    

    def handle_ack(self, message):
        nonce = message.get("nonce")
        self.pending.pop(nonce, None)

    def handle_suspect(self, message):
        node_id = message.get("id")
        if self.suspicion_enabled:
            with self.lock:
                if node_id in self.members:
                    if self.members[node_id].status == "alive":
                        start_suspect(self.members, node_id, time.monotonic(), self.t_suspect)
                        self.log(f"Member {node_id} marked as suspect based on SUSPECT message")
                    else:
                        self.log(f"Received SUSPECT for member {node_id} with status {self.members[node_id].status}")
                else:
                    self.log(f"Received SUSPECT for unknown member {node_id}")

    def handle_delete(self, message):
        target = message.get("target") or message.get("id")
        if not target or target == self.id:
            return        
        with self.lock:
            if target in self.members:
                delete_member(self.members, target)
                self.members[target].last_heard = time.monotonic()
                self.log(f"Member {target} marked as failed")

                if self.mode == "pingack":  #delete from list
                    del self.members[target]
                    if self.pending:
                        self.pending = {n:(mid,dl) for n,(mid,dl) in self.pending.items() if mid != target}
                    self.log(f"Member {target} removed from membership list")
                else: #for gossip, wait for tclenup then remove from list
                    self.members[target].cleanup_timeout = time.monotonic() + self.t_cleanup
                    self.log(f"Member {target} scheduled for removal after cleanup period")
                
                self.broadcast_delete(target)
            else:
                self.broadcast_delete(target)

    def handle_leave(self, message):
        node_id = message.get("id")
        with self.lock:

            if node_id in self.members:
                self.members[node_id].status = "left"
                self.members[node_id].last_heard = time.monotonic()
                self.members[node_id].suspect_deadline = None
                self.members[node_id].cleanup_timeout = time.monotonic() + self.t_cleanup 

                self.piggy.append({"type": "LEAVE", "id": node_id, "status":"left", "incarnation": self.members[node_id].incarnation})
                self.log(f"Member {node_id} has left the cluster")
            else:
                self.log(f"Received LEAVE for unknown member {node_id}")
    
    def send_leave_msg(self):
        piggyback_data = list(self.piggy) 
        leave_message = {
            "type": "LEAVE",
            "id": self.id,
            "status": "left",
            "incarnation": self.members[self.id].incarnation,
            "piggyback": piggyback_data,
        }
        data = json.dumps(leave_message).encode("utf-8")

        with self.lock:
            for mem in self.members.values():
                if mem.id != self.id and mem.status == "alive":
                    try:
                        ip, port = self.parse_id_for_sending(mem.id)
                        if not ip: continue
                        self.sendJSON(leave_message, (ip, port))
                        self.log(f"Sent LEAVE message to {mem.id}")
                    except Exception as e:
                        self.log(f"Failed to send LEAVE message to {mem.id}: {e}")

            if self.id in self.members:
                self.members[self.id].status = "left"
                self.members[self.id].last_heard= time.monotonic()
                self.log(f"Node {self.id} marked as left")

    def control_server(self):
        control_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        control_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        control_sock.bind(("0.0.0.0", 9900))
        control_sock.listen(5) #check
        self.log("Control server listening on 127.0.0.1:9900")

        while True:
            conn, address = control_sock.accept()
            with conn:
                data = conn.recv(4096).decode("utf-8").strip()
                if not data:
                    continue

                data_split = data.split()
                cmd = data_split[0].upper()

                if cmd == "SWITCH":
                    mode_param = data_split[1].lower()
                    suspicion_param = data_split[2].lower()
                    if mode_param == "ping":
                        mode_param = "pingack"
                    self.mode = mode_param

                    if suspicion_param == "suspect":
                        self.suspicion_enabled = True
                    else:
                        self.suspicion_enabled = False
                    self.log(f"Control: switched mode to {self.mode}, suspicion {self.suspicion_enabled}")
                    conn.sendall(b"OK\n")

                elif cmd == "DROP" and len(data_split) == 2:
                    try:
                        drop_num = float(data_split[1])
                        drop_num = max(0.0, min(1.0, drop_num))
                        self.drop = drop_num
                        self.log(f"Control: drop rate set to {self.drop}")
                        conn.sendall(b"OK\n")
                    except Exception:
                        conn.sendall(b"ERR invalid drop value\n")

                elif cmd == "LEAVE":
                    self.send_leave_msg()
                    self.log("Control: node leaving on request")
                    conn.sendall(b"Node Left\n")

                elif cmd == "LIST_MEM":
                    with self.lock:
                        mem_list = ""
                        for m in self.members.values():
                            mem_list += f"Member {m.id}: status={m.status}, incarnation={m.incarnation}\n"
                    conn.sendall((mem_list + "\n").encode())
                
                elif cmd == "LIST_MEM_IDS":
                    self.log("Control: processing LIST_MEM_IDS")
                    members_info = []
                    
                    # 1. Iterate through the member list
                    with self.lock:
                        for m in self.members.values():
                            # 2. Calculate the *consistent* "Ring ID" (hash value)
                            #    (We use the exact same hashing as find_replicas)
                            # 3. Store all required information
                            members_info.append((m.id, m.node_id, m.status, m.incarnation))
                    
                    # 4. Sort as per specification requirements, by "nodeID" (i.e., the m.id string)
                    members_info.sort(key=lambda x: x[0])
                    
                    # 5. Format the output, preserving original content and augmenting it
                    response_lines = []
                    response_lines.append("--- Membership List (Sorted by Node ID) ---")
                    for node_id, node_hash, status, incarnation in members_info:
                        # Preserves the original "status" and "incarnation"
                        response_lines.append(f"Member {node_id}: status={status}, incarnation={incarnation}, RingID: {node_hash}")
                    
                    conn.sendall(("\n".join(response_lines) + "\n").encode())

                elif cmd == "LIST_SELF":
                    conn.sendall((self.id + "\n").encode())

                elif cmd == "JOIN":
                    if self.id in self.members:
                        conn.sendall(b"Already joined\n")
                    else:
                        self.send_join()
                        conn.sendall(b"Join request sent\n")

                elif cmd == "DISPLAY_SUSPECTS":
                    with self.lock:
                        suspects = ""
                        for m in self.members.values():
                            if self.members[m.id].status == "suspect":
                                suspects += f"{m.id}\n"
                    conn.sendall(("\n".join(suspects) + "\n").encode())

                elif cmd == "DISPLAY_PROTOCOL":
                    if self.suspicion_enabled:
                        suspicion_str = "suspect"
                    else:
                        suspicion_str = "nosuspect"

                    mode_map = {
                        "gossip": "gossip",
                        "ping": "ping",          
                        "pingack": "ping",     
                    }
                    mode_str = mode_map.get(self.mode, str(self.mode))

                    combined = f"<{mode_str}, {suspicion_str}>\n"
                    conn.sendall(combined.encode())

                else:
                    conn.sendall(b"ERR unknown command\n")
         
                conn.close()
           
    def receiver(self):
        while True:
            data, addr = self.sock.recvfrom(65535)
            if random.random() < self.drop:
                self.log(f"Dropped incoming packet from {addr}")
                continue
        
            try:
                message = json.loads(data.decode('utf-8'))
                self.handle_msg(message, addr)
            except KeyError as e:
                self.log(f"Receiver exception: missing key {e} in message {message}")
            except Exception as e:
                self.log(f"Receiver exception: {e}")
    
    def bandwidth_loop(self):
        while True:
            time.sleep(5.0)
            now = time.monotonic()
            elapsed = max(1e-6, now - self.last_bw_log)
            bw = self.bytes_out / elapsed
            self.bandwidth_data.append((time.time(), bw, self.mode))
            self.bytes_out = 0
            self.last_bw_log = now
            size = sum(1 for m in self.members.values() if m.status in ("alive", "suspect", "left"))
            self.log(f"EVENT=BW bytes_out_per_s={bw:.2f} mode={self.mode} size={size}")
            # --- Functions for New Threads (ported from server.py and new requirements) ---

    # Helper functions: ported from server.py for TCP communication
    def send_tcp(self, sock, obj):
        data = json.dumps(obj).encode("utf-8")
        hdr = struct.pack("!I", len(data))
        sock.sendall(hdr + data)

    def recv_tcp(self, sock, n):
        buf = bytearray()
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("connection closed")
            buf.extend(chunk)
        return bytes(buf)

    def recv_msg_tcp(self, sock):
        hdr = self.recv_tcp(sock, 4)
        (length,) = struct.unpack("!I", hdr)
        data = self.recv_tcp(sock, length)
        return json.loads(data.decode("utf-8"))
    
    def parse_id_for_sending(self, node_id: str) -> tuple:
     
        try:
           
            # "hostname:port:12345" -> ("hostname:port", "12345")
            host_port, _ = node_id.rsplit(":", 1)
            
            # "hostname:port" -> ("hostname", "port")
            host, port_str = host_port.split(":")
            
            port_str_clean = port_str.split("-")[0]
            
            return host, int(port_str_clean)
            
        except Exception as e:
            self.log(f"CRITICAL: unable to parse ID '{node_id}': {e}")
            return None, None
        
    def write_block(self, remote_file: str, block_name: str, content_bytes: bytes):
            """
            [新] 将一个数据块写入到文件的子目录中。
            例如：DFS/sdfs.txt/block-00001
            """
            # 1. 为文件创建一个目录 (例如 "DFS/sdfs.txt/")
            file_dir = self.file_storage_dir / remote_file
            os.makedirs(file_dir, exist_ok=True)
            
            # 2. 写入块文件
            block_path = file_dir / block_name
            block_path.write_bytes(content_bytes)
            self.log(f"Wrote block: {block_path}")

    def get_next_block_name(self, remote_file: str) -> str:

        file_dir = self.file_storage_dir / remote_file
        if not file_dir.is_dir():
            return "block-00001"
            

        blocks = sorted([p.name for p in file_dir.glob("block-*") if p.is_file()])
        
        if not blocks:

            return "block-00001"
        

        last_block = blocks[-1] # e.g., "block-00002"
        last_num = int(last_block.split("-")[1])
        next_num = last_num + 1
        # 使用 5 位数字填充 (例如 00003)
        return f"block-{next_num:05d}"

    def read_all_blocks(self, remote_file: str) -> bytes:
        """
        [新] 读取一个文件的所有块，按顺序将它们连接起来。
        """
        file_dir = self.file_storage_dir / remote_file
        if not file_dir.is_dir():
            raise FileNotFoundError(f"No such file (directory): {remote_file}")
            
        # 1. 查找所有块并按字母顺序排序
        block_paths = sorted([p for p in file_dir.glob("block-*") if p.is_file()])
        
        full_content = bytearray()
        # 2. 按顺序连接
        for block_path in block_paths:
            full_content.extend(block_path.read_bytes())
            
        return full_content

    def get_local_block_list(self, remote_file: str) -> list:
        """
        [新] 仅获取本地存储的块名称列表。
        """
        file_dir = self.file_storage_dir / remote_file
        if not file_dir.is_dir():
            return []
        return sorted([p.name for p in file_dir.glob("block-*") if p.is_file()])
       
    def get_file_lock(self, filename: str) -> threading.Lock:
        """
        Atomically acquire or create a file-specific lock
        to ensure serialization of write operations.
        """
        with self.global_file_lock:
            if filename not in self.file_locks:
                self.file_locks[filename] = threading.Lock()
            return self.file_locks[filename]

    def file_exists_locally(self, filename: str) -> bool:
        """
        Check if the file exists in the node's local storage path.
        """
        local_path = self.file_storage_dir / filename
        return local_path.is_dir()

    def get_sorted_node_hash_list(self):
        sorted_nodes = []
        with self.lock:
            alive_members = [m for m in self.members.values() if m.status == 'alive']
            if not alive_members:
                return []

            for m in alive_members:
                node_hash = get_hash(m.id)
                sorted_nodes.append((node_hash, m.id))
        
        sorted_nodes.sort()
        return sorted_nodes


    def get_hydfs_files_on_node(self) -> list:
  
            files_with_ids = []


            for f in self.file_storage_dir.iterdir():
           
                if f.is_dir(): 
                    filename = f.name 

                    file_id_hash = get_hash(filename)
                    files_with_ids.append({"filename": filename, "file_id": file_id_hash})

            return files_with_ids
    
    def write_file_from_data(self, filename, data=None):
        dst_path = os.path.join(self.file_storage_dir, filename)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        
        with open(dst_path, "wb") as f:
            f.write(data)
    
    def append_file_from_data(self, filename, data=None):
        dst_path = os.path.join(self.file_storage_dir, filename)
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        
        with open(dst_path, "a") as f:
            f.write(data)

    # Placeholder: You need to implement this consistent hashing routing function
    def find_replicas(self, filename: str) -> list:
        """
        Implements consistent hashing routing.
        Based on the current list of alive members, calculates the N (self.replication_factor) replica nodes where the file should be located.
        
        """
        # 1. Create a temporary, sorted list of node hashes
        sorted_nodes = self.get_sorted_node_hash_list()
        alive_node_ids = [] # Store node IDs
        
        node_hashes = [h for h, nid in sorted_nodes]
        node_ids = [nid for h, nid in sorted_nodes]

        file_hash = get_hash(filename)

        # 5. Find the successor node
        # `bisect_right` finds the index of the first node with hash greater than file_hash
        # This is the core lookup operation of consistent hashing
        start_index = bisect.bisect_right(node_hashes, file_hash)
        
        # 6. Handle ring wrap-around
        # If the hash value is greater than any node on the ring, it belongs to the first node
        if start_index == len(node_ids):
            start_index = 0

        # 7. Collect N (self.replication_factor) replicas
        replicas = []
        
        # Ensure we don't error out if there are too few nodes
        num_nodes = len(node_ids)
        num_replicas = min(self.replication_factor, num_nodes) #
        
        for i in range(num_replicas):
            # (start_index + i) % num_nodes ensures proper wrapping around the ring
            index = (start_index + i) % num_nodes
            replicas.append(node_ids[index])
            
        return replicas

    def replicate_to_followers(self, remote_file, follower_node_ids, file_data_bytes, command,block_name: str = None):
            """
            Sends the file *content* to all follower replicas.
            """
            print(f"[REPLICATION] Starting replication for '{remote_file}' to {len(follower_node_ids)} followers")
            success = True
            for i,node_id in enumerate(follower_node_ids):
                print(f"[REPLICATION] Sending to follower {i+1}: {node_id}")
                try:
                    host, port = self.parse_id_for_sending(node_id)
                    if not host: continue
                    file_port = port + 2
                    print(f"[REPLICATION] Connecting to {host}:{file_port}")

                    with socket.create_connection((host, file_port), timeout=5) as s:
                        # Send content directly instead of reading from disk
                        replica_req = {
                            "command": command,
                            "remote_file": remote_file,
                            "content": file_data_bytes.decode("utf-8") 
                        }
                        if block_name:
                            replica_req["block_name"] = block_name # 添加块名称

                        self.send_tcp(s, replica_req)
                        resp = self.recv_msg_tcp(s)
                        if not resp.get("ok"):
                            success = False
                            self.log(f"[REPL] Replica {node_id} failed: {resp.get('error')}")
                except Exception as e:
                    success = False
                    self.log(f"[REPL] Could not contact replica {node_id}: {e}")
            return success

    def receive_file_stream(self, conn, dst_path, file_size):
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        remaining = file_size
        with open(dst_path, "wb") as f:
            while remaining > 0:
                chunk = conn.recv(min(64 * 1024, remaining))
                if not chunk:
                    break
                f.write(chunk)
                remaining -= len(chunk)
        self.log(f"[RECV] Wrote {file_size - remaining} bytes to {dst_path}")

    def clean_storage_on_startup(self):
        """Clear file storage when node starts up"""
        if os.path.exists(self.file_storage_dir):
            self.log(f"Cleaning storage directory: {self.file_storage_dir}")
            # Delete all files but preserve directory structure
            for file_path in self.file_storage_dir.glob("*"):
                if file_path.is_file():
                    file_path.unlink()
                elif file_path.is_dir():
                    shutil.rmtree(file_path)
            self.log("Storage directory cleaned")


    # New Thread 1 logic: Handle file clients (from server.py)
    def handle_file_client_verification(self, conn, addr):
        try:
            req = self.recv_msg_tcp(conn)
            filename = req.get("filename")
            print(f"[SERVER] Received {req.get('command')} command for file: {filename}")

            dfs_dir = pathlib.Path(self.file_storage_dir)
            file_path = dfs_dir / filename
            if file_path.exists():
                return self.send_tcp(conn, {"ok": True, "error": "File already exists"})
            return self.send_tcp(conn, {"ok": False, "error": "File doesn't exist"})
        except Exception as e:
            self.log(f"Handle file verification client error: {e}")
            try:
                # Ensure client doesn't wait forever
                self.send_tcp(conn, {"ok": False, "error": str(e)})
            except:
                pass # Connection might already be closed
        finally:
            conn.close()

    # New Thread 1 logic: Handle file clients (from server.py)
    def handle_file_client(self, conn, addr):
        
        try:
            # 1. Receive client request (e.g.: {"command": "append", "remote_file": "file.txt", ...})
            print(f"[SERVER] New connection from {addr}")
            req = self.recv_msg_tcp(conn)       
            command = req.get("command")
            remote_file = req.get("remote_file") # The HyDFS filename the client wants to operate on
            content = req.get("content") # The HyDFS filename the client wants to operate on
            secondary_replica = int(req.get("secondary_replica", 0))
            print(f"Received request from {addr}: {command} {remote_file}")
            # 2. Find replicas (consistent hashing)
            print(f"Client: {addr}")
            print(f"Command: {command}")
            print(f"Remote file: {remote_file}")
            print(f"This node: {self.id}")
            # This step is O(1) routing
            replicas = self.find_replicas(remote_file)
            print(f"[ROUTING] File '{remote_file}' replicas: {replicas}")
            print(f"[ROUTING] Primary replica: {replicas[0]}, This node: {self.id}")
            if not replicas:
                print(f"[FORWARD] Forwarding to primary: {primary_node_id}")
                self.send_tcp(conn, {"ok": False, "error": "No nodes available"})
                return
         

            # 3. Determine primary replica (Leader)
            # We agree: the first node on the hash ring (replicas[0]) is the primary replica for this file
            primary_node_id = replicas[0]
            follower_node_ids = replicas[1:] # The rest are followers (n=3)
            print(f"[ROUTING] Primary replica: {primary_node_id}, This node: {self.id}")


            internal_commands = [
                "append_replica", 
                "create_replica",
                "getfromreplica",            
                "_internal_merge_check" # (你当前的 merge 逻辑)
            ]
            
            if command not in internal_commands and self.id != primary_node_id:
                # [非主副本]：转发请求
                self.forward_file_to_primary(primary_node_id, req, conn)
                return

            #     # This is to satisfy (iii) read-your-writes consistency
            #     self.log(f"Forwarding {command} for {remote_file} -> {primary_node_id}")
            #     # ... (Implement forwarding logic: connect to primary_node_id, send req) ...
            #     # ... (Receive response from primary_node_id and send it back unchanged to original client) ...
            #     return

            # 5. Execution logic: [This node is the primary replica]
            # (At this point, we can ensure all read/write operations go through this node, satisfying (iii) read-your-writes)
            
            # --- To satisfy (ii) eventual consistency, primary replica must serialize operations on the same file ---
            # --- You need a file lock ---
            with self.get_file_lock(remote_file): # This is a lock function you need to implement
                if command == "create":
                    # 规范 ：只有第一次 create 应该成功
                    if self.file_exists_locally(remote_file): # 现在检查目录
                        print(f"CREATE: Completed with error: File already exists")
                        self.send_tcp(conn, {"ok": False, "error": "File already exists"})
                    else:
                        content_str = req.get("content", "")
                        content_bytes = content_str.encode("utf-8")
                        
                        # 1. 定义第一个块的名称
                        block_name = "block-00001"
                        
                        # 2. 写入第一个块
                        self.write_block(remote_file, block_name, content_bytes)

                        # 3. 复制到跟随者 (我们必须告诉它们块的名称)
                        success = self.replicate_to_followers(
                            remote_file, 
                            follower_node_ids, 
                            content_bytes, 
                            "create_replica", # 内部命令
                            block_name        # 额外参数
                        )
                        
                        if success:
                            self.log(f"CREATE: {remote_file} successful and replicated.")
                            print(f"CREATE: Completed request for '{remote_file}'") #
                            self.send_tcp(conn, {"ok": True, "replicas": replicas})
                        else:
                            self.log(f"CREATE: {remote_file} created locally, but replication failed.")
                            print(f"CREATE: Completed with error: Replication failed") #
                            self.send_tcp(conn, {"ok": False, "error": "Failed to replicate file to all followers"})

                elif command == "append":
                   # 规范：Append 要求文件必须存在
                    if not self.file_exists_locally(remote_file): # 检查目录
                        print(f"APPEND: Completed with error: File does not exist")
                        self.send_tcp(conn, {"ok": False, "error": "File does not exist"})
                    else:
                        content_str = req.get("content", "")
                        content_bytes = content_str.encode("utf-8")
                        
                        # 1. 计算下一个块的名称
                        block_name = self.get_next_block_name(remote_file)
                        
                        # 2. 写入这个新块
                        self.write_block(remote_file, block_name, content_bytes)

                        # 3. 复制这个新块
                        # (我们重用 'append_replica' 命令，但它现在也需要 block_name)
                        success = self.replicate_to_followers(
                            remote_file, 
                            follower_node_ids, 
                            content_bytes, 
                            "append_replica", # 内部命令
                            block_name        # 额外参数
                        )
                        
                        if success:
                            self.log(f"APPEND: {remote_file} successful and replicated.")
                            self.send_tcp(conn, {"ok": True, "replicas": replicas})
                        else:
                            self.log(f"APPEND: {remote_file} appended locally, but replication failed.")
                            self.send_tcp(conn, {"ok": False, "error": "Failed to replicate file to all followers"})

                        # 1. Receive data to append from client
                        # 2. Append data to local file
                        # 3. Forward "append" command and appended data to all follower_node_ids
                        #    (This ensures (ii) eventual consistency because you determine the order)
                        # 4. Wait for all followers to acknowledge "OK"
                        # 5. Send "OK" to original client
                        #    (This ensures (i) client append order)
                
                elif command == "append_replica":
                    print(f"Received request from Primary: append_replica {remote_file}")
                    
                    if not self.file_exists_locally(remote_file): # 检查目录
                        print(f"APPEND_REPLICA: Completed with error: File does not exist")
                        self.send_tcp(conn, {"ok": False, "error": "[REPLICA] File doesn't exist."})
                    else:
                        content_str = req.get("content", "")
                        content_bytes = content_str.encode("utf-8")
                        
                        # 1. 接收块名称 (如果主副本没有发送，就自己计算)
                        block_name = req.get("block_name")
                        if not block_name:
                            block_name = self.get_next_block_name(remote_file)
                            
                        # 2. 写入这个新块
                        self.write_block(remote_file, block_name, content_bytes)
                        
                        self.log(f"[REPLICA] Append: {remote_file}/{block_name} successful")
                        print(f"APPEND_REPLICA: Completed request for '{remote_file}/{block_name}'")
                        self.send_tcp(conn, {"ok": True})

                elif command == "get":
                    # The routing logic has already ensured we are the primary replica.
                    # This satisfies (iii) Read-my-writes.
                    
                    self.log(f"GET: Processing get for '{remote_file}'")
                        
                    try:
                        # 1. 读取并连接所有块
                        content_bytes = self.read_all_blocks(remote_file)
                        content = content_bytes.decode("utf-8") # 假设是 utf-8
                            
                        self.log(f"GET: {remote_file} successful. Sending {len(content)} bytes.")
                        print(f"GET: Completed request for '{remote_file}'")
                        self.send_tcp(conn, {"ok": True, "file_data": content})
                        
                    except FileNotFoundError:
                        self.log(f"GET: Error - File not found: {remote_file}")
                        print(f"GET: Completed with error: File not found")
                        self.send_tcp(conn, {"ok": False, "error": "File not found"})
                            
                    except Exception as e:
                            self.log(f"GET: Error reading file {remote_file}: {e}")
                            print(f"GET: Completed with error: {e}") #
                            self.send_tcp(conn, {"ok": False, "error": f"Error reading file: {e}"})

                elif command == "getfromreplica":
                        # 这个命令会绕过主副本路由
                        self.log(f"GETFROMREPLICA: Processing request for '{remote_file}'")
                        print(f"GETFROMREPLICA: Received request for '{remote_file}'") 

                        try:
                            # 1. [修改] 读取并连接所有块
                            content_bytes = self.read_all_blocks(remote_file)
                            content = content_bytes.decode("utf-8") # 假设是 utf-8

                            self.log(f"GETFROMREPLICA: {remote_file} successful. Sending {len(content)} bytes.")
                            print(f"GETFROMREPLICA: Completed request for '{remote_file}'") 
                            self.send_tcp(conn, {"ok": True, "file_data": content})

                        except FileNotFoundError:
                            self.log(f"GETFROMREPLICA: Error - File not found: {remote_file}")
                            print(f"GETFROMREPLICA: Completed with error: File not found") 
                            self.send_tcp(conn, {"ok": False, "error": "File not found on this replica"})
                        except Exception as e:
                            self.log(f"GETFROMREPLICA: Error reading file {remote_file}: {e}")
                            print(f"GETFROMREPLICA: Completed with error: {e}") 
                            self.send_tcp(conn, {"ok": False, "error": f"Error reading file: {e}"})

                elif command == "ls":
                    self.log(f"LS: Processing ls for '{remote_file}'")
                    try:
                        found_replicas = []
                        for replica in replicas:
                            if self.id == replicas[0]:
                                file_hash = get_hash(remote_file)
                                if self.file_exists_locally(remote_file):
                                    found_replicas.append((replica, get_hash(replica),))
                                else:
                                    self.send_tcp(conn, {"ok": False, "error": "File not found."})
                                    return
                            else:
                                try:
                                    host, port_str = replica.split(":")
                                    self.log(f"[VERIFICATION] Connecting to {host}:{int(port_str) + 2}")

                                    with socket.create_connection((host, int(port_str) + 2), timeout=5) as s:
                                        # Send content directly instead of reading from disk
                                        self.send_tcp(s, {
                                            "command": "ls_replica",
                                            "remote_file": remote_file,
                                        })
                                        resp = self.recv_msg_tcp(s)
                                        if resp.get("ok"):
                                            found_replicas.append((replica, get_hash(replica),))
                                except Exception as e:
                                    self.log(f"[REPL] Could not contact replica {replica}: {e}")

                        self.log(f"Message: {found_replicas}")
                        self.send_tcp(conn, {"ok": True, "file_id": file_hash, "replicas": found_replicas})
                        # for replica in replicas:
                        #     self.log(f"Replicas: {replicas}")
                        # self.send_tcp(conn, {"ok": True, "file_id": file_hash, "replicas": replicas})
                        # return
                        # self.log(f"LS: FileID={file_hash}, Replicas={replicas}")
                        # self.send_tcp(conn, {"ok": True, "file_id": file_hash, "replicas": replicas})
                    
                    except Exception as e:
                        self.log(f"LS: Error processing ls: {e}")
                        print(f"LS: Completed with error: {e}")
                        self.send_tcp(conn, {"ok": False, "error": str(e)})       
                
                elif command == "ls_replica":
                    self.log(f"LS: Processing ls_replica for '{remote_file}'")
                    try:
                        file_hash = get_hash(remote_file)
                        if self.file_exists_locally(remote_file):
                            self.send_tcp(conn, {"ok": True, "file_id": file_hash, "replicas": replicas})
                        else:
                            self.send_tcp(conn, {"ok": False, "error": "File not found."})
                    except Exception as e:
                        self.log(f"LS: Error processing ls: {e}")
                        print(f"LS: Completed with error: {e}")
                        self.send_tcp(conn, {"ok": False, "error": str(e)})       

                elif command == "merge":
                    # The routing logic already guarantees we are the primary replica.
                    self.log(f"MERGE: Processing merge for '{remote_file}'")
                    print(f"MERGE: Received request for '{remote_file}'") # Demo log

                    # Canonical assumption: no new updates during merge [cite: 55]
                    if not self.file_exists_locally(remote_file):
                        self.log(f"MERGE: Error - File not found: {remote_file}")
                        print(f"MERGE: Completed with error: File not found") # Demo log
                        self.send_tcp(conn, {"ok": False, "error": "File not found"})
                    else:
                        # 1. 获取主副本的块列表
                        primary_blocks = self.get_local_block_list(remote_file)
                        self.log(f"MERGE: Primary has blocks: {primary_blocks}")
                        
                        # 2. 遍历所有跟随者，命令它们检查
                        all_success = True
                        for node_id in follower_node_ids:
                            try:
                                host, port = self.parse_id_for_sending(node_id)
                                if not host: continue
                                
                                with socket.create_connection((host, self.file_system_port), timeout=10) as s:
                                    # 3. 发送主副本的块列表
                                    self.send_tcp(s, {
                                        "command": "_internal_merge_check",
                                        "remote_file": remote_file,
                                        "primary_blocks": primary_blocks
                                    })
                                    
                                    # 4. 等待跟随者回复它 *缺少* 的块
                                    resp = self.recv_msg_tcp(s)
                                    if not resp.get("ok"):
                                        all_success = False; continue
                                    
                                    missing_blocks = resp.get("missing_blocks", [])
                                    if not missing_blocks:
                                        self.log(f"MERGE: Follower {node_id} is already in sync.")
                                        continue
                                        
                                    self.log(f"MERGE: Follower {node_id} is missing {missing_blocks}")
                                    
                                    # 5. *只*发送缺失的块
                                    for block_name in missing_blocks:
                                        block_path = (self.file_storage_dir / remote_file) / block_name
                                        content_bytes = block_path.read_bytes()
                                        
                                        # 我们重用 create_replica 来发送块
                                        self.replicate_to_followers(
                                            remote_file, 
                                            [node_id], # 只发送给这个 follower
                                            content_bytes, 
                                            "create_replica", 
                                            block_name
                                        )

                            except Exception as e:
                                self.log(f"MERGE: Error merging with {node_id}: {e}")
                                print(f"MERGE: Error merging with {node_id}: {e}")
                                all_success = False
                            
                        if all_success:
                                self.log(f"MERGE: {remote_file} successful.")
                                print(f"MERGE: Completed request for '{remote_file}'") # 演示日志
                                self.send_tcp(conn, {"ok": True, "message": "Merge complete"})
                        else:
                                self.log(f"MERGE: Error - Failed to merge all replicas.")
                                print(f"MERGE: Completed with error: Failed to merge all replicas") # 演示日志
                                self.send_tcp(conn, {"ok": False, "error": "Failed to merge all replicas"})
                           

                elif command == "_internal_merge_check":
                    # 这是来自主副本的 merge 检查
                    remote_file = req.get("remote_file")
                    primary_blocks = set(req.get("primary_blocks", []))
                    
                    # 1. 检查本地块
                    local_blocks = set(self.get_local_block_list(remote_file))
                    
                    # 2. 计算差异
                    missing_blocks = list(primary_blocks - local_blocks)
                    
                    self.log(f"[MERGE_CHECK] Missing blocks: {missing_blocks}")
                    print(f"[MERGE_CHECK] Missing blocks: {missing_blocks}")
                    
                    # 3. 回复主副本
                    self.send_tcp(conn, {"ok": True, "missing_blocks": missing_blocks})

                elif command == "create_replica":
                    print(f"Received request from Primary: create_replica {remote_file}")
                    
                    content_str = req.get("content", "")
                    # 接收块名称 (如果不存在，则默认为 'block-00001' 以兼容旧版)
                    block_name = req.get("block_name", "block-00001") 
                    
                    if content_str:
                        content_bytes = content_str.encode("utf-8")
                        # 写入特定的块
                        self.write_block(remote_file, block_name, content_bytes)
                        
                        print(f"Completed request: create_replica {remote_file}/{block_name}")
                        self.send_tcp(conn, {"ok": True})
                    else:
                        print(f"[CREATE_REPLICA] Error: Empty content")
                        self.send_tcp(conn, {"ok": False, "error": "[REPLICA] Empty content"})
                            


                elif command == "liststore":
                    self.send_tcp(conn, {"ok": True, "node_id": get_hash(self.id), "files": self.get_hydfs_files_on_node()})

        except Exception as e:
            self.log(f"Handle file client error: {e}")
            try:
                # Ensure client doesn't wait forever
                self.send_tcp(conn, {"ok": False, "error": str(e)})
            except:
                pass # Connection might already be closed
        finally:
            conn.close()
    # New Thread 1 target: File system server (from server.py)
    def tcp_file_server(self):
        """
        This is the target for new thread 1. It listens for file requests on TCP port (e.g., 9002).
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", self.file_system_port))
            s.listen(128)
            self.log(f"[HyDFS Server] Listening for file requests on TCP port {self.file_system_port}...")
            print(f"[HyDFS Server] Listening for file requests on TCP port {self.file_system_port}...")
            
            while True:
                conn, addr = s.accept()
                # Start a *new* worker thread for each client connection
                t = threading.Thread(
                    target=self.handle_file_client,
                    args=(conn, addr),
                    daemon=True
                )
                t.start()
    
    def tcp_file_server_verification(self):
        """
        This is the target for new thread 3. It listens for file validations on TCP port (e.g., 9003).
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("0.0.0.0", self.file_replica_verification_port))
            s.listen(128)
            self.log(f"[HyDFS Server] Listening for file verifications on TCP port {self.file_replica_verification_port}...")
            print(f"[HyDFS Server] Listening for file verifications on TCP port {self.file_replica_verification_port}...")
            
            while True:
                conn, addr = s.accept()
                # Start a *new* worker thread for each client connection
                t = threading.Thread(
                    target=self.handle_file_client_verification,
                    args=(conn, addr),
                    daemon=True
                )
                t.start()

    # New Thread 2 target: Rebalance/Replication Manager
    def replication_manager(self):
               
            self.log("[Replication Manager] Replication manager started.")
            while True:
                time.sleep(8)

                current_members = set()
                try:
                    with self.lock:
                        current_members = set(m.id for m in self.members.values() if m.status == 'alive')
                    
                    if current_members == self.previous_members:
                        continue # 没有变化

                    self.log(f"Membership change detected: {len(self.previous_members)} -> {len(current_members)}")

                    # 遍历本地*所有*文件（现在是目录）
                    local_file_dirs = [d for d in self.file_storage_dir.iterdir() if d.is_dir()]
                    
                    if not local_file_dirs:
                        self.log("No local files, rebalancing not needed.")
                    
                    for file_dir in local_file_dirs:
                        filename = file_dir.name # e.g., "sdfs-test-2.txt"
                        
                        # 1. 计算*新*的副本位置
                        new_replicas = self.find_replicas(filename)
                        
                        # 2. 检查再平衡（Re-balancing）
                        if self.id not in new_replicas:
                            self.log(f"Rebalancing: {filename} no longer belongs to this node.")
                            # shutil.rmtree(file_dir)
                            continue

                        # 3. 检查再复制（Re-replication）
                        # (只在主副本上执行)
                        if self.id == new_replicas[0]:
                            self.log(f"Re-replication check for: {filename}")
                
                            # 1. 获取主副本（我们）的块列表
                            primary_blocks = self.get_local_block_list(filename)
                            
                            # 2. 检查*所有*跟随者
                            for node_id in new_replicas[1:]:
                                if node_id == self.id: continue # 以防万一
                                
                                try:
                                    # 3. 检查跟随者缺少哪些块
                                    host, port = self.parse_id_for_sending(node_id)
                                    if not host: continue
                                    
                                    with socket.create_connection((host, self.file_system_port), timeout=5) as s:
                                        self.send_tcp(s, {
                                            "command": "_internal_merge_check",
                                            "remote_file": filename,
                                            "primary_blocks": primary_blocks
                                        })
                                        resp = self.recv_msg_tcp(s)
                                        if not resp.get("ok"): continue

                                        missing_blocks = resp.get("missing_blocks", [])
                                        if missing_blocks:
                                            self.log(f"[REPL-MGR] Node {node_id} is missing {missing_blocks}")
                                            # 5. *只*发送缺失的块
                                            for block_name in missing_blocks:
                                                block_path = file_dir / block_name
                                                content_bytes = block_path.read_bytes()
                                                self.replicate_to_followers(
                                                    filename, 
                                                    [node_id], 
                                                    content_bytes, 
                                                    "create_replica", 
                                                    block_name
                                                )
                                except Exception as e:
                                    self.log(f"[REPL-MGR] Could not re-replicate to {node_id}: {e}")
                        
                    # 7. 更新状态以备下次检查
                    self.previous_members = current_members
                    self.log("Rebalancing/re-replication check completed.")

                except Exception as e:
                    self.log(f"Replication Manager error: {e}")

    def forward_file_to_primary(self, primary_node_id: str, req: dict, client_conn):
    
        try:
            # 1. Extract the host and file port of the primary replica
            host, port = self.parse_id_for_sending(primary_node_id)
            if not host:
                raise Exception(f"Could not parse primary_node_id: {primary_node_id}")
            file_port = port + 2

            self.log(f"[FORWARD] Forwarding {req.get('command')} to primary replica {host}:{file_port}")

            # 2. Establish a new connection to the primary replica
            with socket.create_connection((host, file_port), timeout=10) as s:
                s.settimeout(10)
                
                # 3. Forward the client's *original req* (now containing 'content') to the primary
                self.send_tcp(s, req)
                
                # 4. Wait for the primary replica's *final* response (e.g., {"ok": True, "replicas": [...]})
                resp = self.recv_msg_tcp(s)
                
                # 5. Relay the primary replica's response unchanged back to the *original client*
                self.send_tcp(client_conn, resp)

        except Exception as e:
            self.log(f"[FORWARD ERROR] Failed to forward to {primary_node_id}: {e}")
            try:
                # Ensure the original client doesn't get stuck
                self.send_tcp(client_conn, {"ok": False, "error": f"Failed to forward request to primary: {e}"})
            except Exception:
                pass  # Original client connection may have been closed


def main():
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

    if not daemon.id.startswith(daemon.introducer):
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
