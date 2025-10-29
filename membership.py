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

class Member:
    def __init__(self, id, heartbeat, last_heard, status, incarnation):
        self.id = id #node id
        self.heartbeat = heartbeat #for gossip specifically
        self.last_heard = last_heard if last_heard is not None else time.monotonic()
        self.status = status # "alive", "suspect", "failed", "left"
        self.incarnation = incarnation 
        self.suspect_deadline = None 
        self.cleanup_timeout = None #for gossip

class Daemon:
    def __init__(self, hostname, introducer, port, mode, drop,
                 t_fail, t_cleanup, t_suspect, heartbeat_interval):
        self.port = port
        self.hostname = hostname 
        self.id = f"{hostname}:{port}"
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
        # 2. Define the file storage path (e.g.: ./files_9000/)
        self.file_storage_dir = pathlib.Path(f"DFS")
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
        print(f"[{timestamp}] {message}", flush=True)

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
                ip, port_ts = m.id.split(":")
                port = int(port_ts.split("-")[0])
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
        try:
            ip, port_timestamp = member.id.split(":")
            port = int(port_timestamp.split("-")[0])
        except Exception as e:
            print(f"Invalid member id format for {member.id}: {e}")
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

            ip = peers[0].id.split(":")[0]
            port = int(peers[0].id.split(":")[1].split("-")[0])
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
        if self.id != self.introducer:
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
            ip, port_timestamp = node_id.split(":")
            port = int(port_timestamp.split("-")[0])
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
                self.log(f"Received DELETE for unknown member {target}")
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
                        ip, port_timestamp = mem.id.split(":")
                        port = int(port_timestamp.split("-")[0])
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
        local_path = self.file_storage_path / filename
        return local_path.exists()

    def get_sorted_node_hash_list(self):
        sorted_nodes = []
        with self.lock:
            alive_members = [m for m in self.members.values() if m.status == 'alive']
            if not alive_members:
                return []

            for m in alive_members:
                node_hash = int(hashlib.sha1(m.id.encode('utf-8')).hexdigest(), 16)
                sorted_nodes.append((node_hash, m.id))
        
        sorted_nodes.sort()
        return sorted_nodes

    def get_file_lock(self, filename):
        if filename not in self.file_locks:
            self.file_locks[filename] = threading.Lock()
        return self.file_locks[filename]

    def file_exists_locally(self, filename):
        return os.path.exists(os.path.join(self.file_storage_dir, filename))

    def write_file_locally(self, filename, remote_file, data=None):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        src = os.path.join(base_dir, filename)
        dst = os.path.join(self.file_storage_dir, remote_file)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy(src, dst)

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

        file_hash = int(hashlib.sha1(filename.encode('utf-8')).hexdigest(), 16)

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

    def replicate_to_followers(self, remote_file, follower_node_ids):
        """
        Sends the specified file to all follower replicas and waits for their acknowledgements.
        Returns True if all followers acknowledged successfully, False otherwise.
        """
        success = True
        file_path = os.path.join(self.file_storage_dir, remote_file)

        try:
            with open(file_path, "rb") as f:
                file_data = f.read()
        except Exception as e:
            self.log(f"Replication error: cannot read local file {remote_file}: {e}")
            return False

        for node_id in follower_node_ids:
            try:
                host, _ = node_id.split(":")

                with socket.create_connection((host, self.file_system_port), timeout=5) as s:
                    self.send_tcp(s, {
                        "command": "create_replica",
                        "remote_file": remote_file,
                        "file_data": file_data.decode("latin1")  # safe round-trip for binary
                    })
                    resp = self.recv_msg_tcp(s)
                    if not resp.get("ok"):
                        success = False
                        self.log(f"[REPL] Replica {node_id} failed: {resp}")
            except Exception as e:
                success = False
                self.log(f"[REPL] Could not contact replica {node_id}: {e}")

        return success

    # New Thread 1 logic: Handle file clients (from server.py)
    def handle_file_client(self, conn, addr):
        try:
            # 1. Receive client request (e.g.: {"command": "append", "remote_file": "file.txt", ...})
            req = self.recv_msg_tcp(conn)
            command = req.get("command")
            remote_file = req.get("remote_file") # The HyDFS filename the client wants to operate on
            local_file = req.get("local_file") # The HyDFS filename the client wants to operate on
            # 2. Find replicas (consistent hashing)
            # This step is O(1) routing
            replicas = self.find_replicas(remote_file)
            if not replicas:
                self.send_tcp(conn, {"ok": False, "error": "No nodes available"})
                return

            # 3. Determine primary replica (Leader)
            # We agree: the first node on the hash ring (replicas[0]) is the primary replica for this file
            primary_node_id = replicas[0]
            follower_node_ids = replicas[1:] # The rest are followers (n=3)

            # 4. Routing logic: Check if this node is the primary replica
            if self.id != primary_node_id and command != "create_replica":
                # [Not primary replica]: Forward request to the real primary replica
                # This is to satisfy (iii) read-your-writes consistency
                self.log(f"Forwarding {command} for {remote_file} -> {primary_node_id}")
                # ... (Implement forwarding logic: connect to primary_node_id, send req) ...
                # ... (Receive response from primary_node_id and send it back unchanged to original client) ...
                return

            # 5. Execution logic: [This node is the primary replica]
            # (At this point, we can ensure all read/write operations go through this node, satisfying (iii) read-your-writes)
            
            # --- To satisfy (ii) eventual consistency, primary replica must serialize operations on the same file ---
            # --- You need a file lock ---
            with self.get_file_lock(remote_file): # This is a lock function you need to implement
                if command == "create":
                    if self.file_exists_locally(remote_file):
                        self.send_tcp(conn, {"ok": False, "error": "File already exists"})
                    else:
                        data = {}
                        self.write_file_locally(local_file, remote_file, data)
                        if self.replicate_to_followers(remote_file, follower_node_ids):
                            self.log(f"CREATE: {remote_file} replicated successfully")
                            self.send_tcp(conn, {"ok": True})
                        else:
                            self.send_tcp(conn, {"ok": False, "error": "Replication failed"})
                        self.log(f"CREATE: {remote_file} successful")
                        self.send_tcp(conn, {"ok": True})

                elif command == "append":
                    # Check if file exists
                    if not self.file_exists_locally(remote_file):
                        self.send_tcp(conn, {"ok": False, "error": "File does not exist"})
                    else:
                        # 1. Receive data to append from client
                        # 2. Append data to local file
                        # 3. Forward "append" command and appended data to all follower_node_ids
                        #    (This ensures (ii) eventual consistency because you determine the order)
                        # 4. Wait for all followers to acknowledge "OK"
                        # 5. Send "OK" to original client
                        #    (This ensures (i) client append order)
                        self.log(f"APPEND: {remote_file} successful")
                        self.send_tcp(conn, {"ok": True})

                elif command == "get":
                    #
                    # (Because we are the primary replica, we guarantee having the latest data)
                    if not self.file_exists_locally(remote_file):
                        self.send_tcp(conn, {"ok": False, "error": "File not found"})
                    else:
                        # 1. Read file data locally
                        # 2. Send file data back to client
                        self.log(f"GET: {remote_file} successful")
                        self.send_tcp(conn, {"ok": True, "file_data": ...})

                elif command == "merge":
                    #
                    # Assumption: No new updates are occurring
                    # Because primary replica always has the "correct" order (all writes must go through it)
                    # So the merge logic is: primary replica forces its version to overwrite all followers
                    
                    # 1. Read *entire* file content locally
                    # 2. Send *entire* content to all follower_node_ids (using an internal command like "FORCE_OVERWRITE")
                    # 3. Wait for followers to acknowledge "OK"
                    # 4. Send "OK" to client
                    self.log(f"MERGE: {remote_file} successful")
                    self.send_tcp(conn, {"ok": True, "message": "Merge complete"})

                if command == "create_replica":
                    if self.file_exists_locally(remote_file):
                        self.send_tcp(conn, {"ok": False, "error": "[REPLICA] File already exists"})
                    else:
                        self.write_file_locally(local_file, remote_file, data)
                        self.log(f"[REPLICA] CREATE: {remote_file} successful")
                        self.send_tcp(conn, {"ok": True})
                    
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
            
            while True:
                conn, addr = s.accept()
                # Start a *new* worker thread for each client connection
                t = threading.Thread(
                    target=self.handle_file_client,
                    args=(conn, addr),
                    daemon=True
                )
                t.start()

    # New Thread 2 target: Rebalance/Replication Manager
    def replication_manager(self):
        """
        This is the target for new thread 2. It periodically checks for membership changes and triggers rebalancing.
       
        """
        self.log("[Replication Manager] Replication manager started.")
        while True:
            # 1. Wake up periodically
            time.sleep(10) # Check every 10 seconds

            current_members = set()
            try:
                # 2. Get current list of alive members
                with self.lock:
                    current_members = set(m.id for m in self.members.values() if m.status == 'alive')
                
                # 3. Check if different from last recorded state
                if current_members == self.previous_members:
                    continue # No membership change, skip

                self.log(f"Membership change detected: {len(self.previous_members)} -> {len(current_members)}")

                # --- This is complex logic you need to implement ---
                # 4. Iterate through all files stored locally on this node
                local_files = list(self.file_storage_dir.glob("*"))
                if not local_files:
                    self.log("No local files, rebalancing not needed.")
                
                for file_path in local_files:
                    filename = file_path.name
                    # 5. Calculate correct replica locations based on *new* list
                    new_replicas = self.find_replicas(filename)
                    
                    # 6. Check if this node needs to move data
                    if self.id not in new_replicas:
                        # This node is no longer a replica, should send file to new replicas and delete local file
                        self.log(f"Rebalancing: {filename} no longer belongs to this node. (not implemented)")
                        # ... (Implement logic to send file to new_replicas) ...
                        # os.remove(file_path) # Delete after successful transfer

                    elif self.id == new_replicas[0]: # Assuming this node is primary replica
                        # This node is still primary replica, need to ensure other replicas have data (re-replication)
                        self.log(f"Re-replication: Checking replicas {new_replicas} for {filename} (not implemented)")
                        # ... (Implement logic to check and send file to new_replicas[1] and [2]) ...

                # 7. Update state for next check
                self.previous_members = current_members
                self.log("Rebalancing/re-replication check completed.")

            except Exception as e:
                self.log(f"Replication Manager error: {e}")

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
