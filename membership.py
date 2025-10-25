import argparse
from collections import deque
import json
import random
import socket
import threading
import time
from typing import Dict
from suspicion import start_suspect, clear_suspect, delete_member

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

    def log(self, message: str):
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"[{timestamp}] {message}")

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
            try:
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
            except Exception as e:
                import traceback
                self.log("Control server exception:\n" + traceback.format_exc())
           
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

    # For short lived experiments
    # time.sleep(60)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down Daemon.")
        
    print("\n=== Bandwidth Data ===")
    for timestamp, bw, mode in daemon.bandwidth_data:
        print(f"{timestamp},{bw},{mode}")

if __name__ == "__main__":
    main()
