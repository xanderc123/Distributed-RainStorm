# MP2 Demo Commands (10 VMs)

This README contains **copy-pasteable** commands to reproduce the demo on 10 VMs (`fa25-cs425-a801` … `fa25-cs425-a810`) with:

* UDP **port** `9000` (and `9001` only for the rejoin test)
* **Introducer**: `fa25-cs425-a801.cs.illinois.edu:9000`
* Start in **PingAck without Suspicion**, then switch to **Gossip**, then enable **Suspicion**

> Replace `/path/to/mp2` with your repo path if needed.
> All `control.py` calls talk to the local control port `9900` on each VM.

---

## Prep / Kill (run **on each VM** in its own terminal)

```bash
pkill -f "membership.py.*--port 9000" || true
ss -lnpt | egrep ':9900' || echo 'no control 9900'
ss -lnup | egrep ':9000' || echo 'no udp 9000'
```

---

## Test 1 — Join (PingAck, No Suspicion)

### Introducer (vm1 = `fa25-cs425-a801.cs.illinois.edu`)

```bash
cd /path/to/mp2
nohup python3 membership.py \
  --port 9000 \
  --introducer fa25-cs425-a801.cs.illinois.edu:9000 \
  --mode pingack --drop 0.0 --t_fail 2 --t_suspect 1.8 --t_cleanup 2 \
  > daemon_vm1.log 2>&1 & echo $! > daemon_vm1.pid
sleep 1
python3 control.py display_protocol
python3 control.py list_self
python3 control.py list_mem
```

### Group A join **simultaneously** (vm2–vm5)

Run on **each** of vm2, vm3, vm4, vm5:

```bash
cd /path/to/mp2
nohup python3 membership.py \
  --port 9000 \
  --introducer fa25-cs425-a801.cs.illinois.edu:9000 \
  --mode pingack --drop 0.0 --t_fail 2 --t_suspect 1.8 --t_cleanup 2 \
  > vm.log 2>&1 & echo $! > vm.pid
```

#### Halfway list (5 members)

On **vm1** and **vm3**:

```bash
python3 control.py list_mem
```

### Group B join **20s later** (vm6–vm10)

Run on **each** of vm6, vm7, vm8, vm9, vm10:

```bash
cd /path/to/mp2
nohup python3 membership.py \
  --port 9000 \
  --introducer fa25-cs425-a801.cs.illinois.edu:9000 \
  --mode pingack --drop 0.0 --t_fail 2 --t_suspect 1.8 --t_cleanup 2 \
  > vm.log 2>&1 & echo $! > vm.pid
```

#### Final list (all 10)

On **vm2** and **vm8**:

```bash
python3 control.py list_mem
```

---

## Test 2 — Failures (PingAck, No Suspicion)

### Kill two **non-introducer** nodes (example: vm4 & vm7)

On **vm4**:

```bash
pkill -f "membership.py.*--port 9000"
```

On **vm7**:

```bash
pkill -f "membership.py.*--port 9000"
```

### After 10s, show membership (should list **8**)

On **vm3** and **vm9**:

```bash
python3 control.py list_mem
```

---

## Test 3 — Switch to Gossip, then Enable Suspicion

### Switch **all alive nodes** (vm1, vm2, vm3, vm5, vm6, vm8, vm9, vm10)

**Switch to Gossip (no suspicion):**

```bash
python3 control.py switch gossip nosuspect
python3 control.py display_protocol
```

**Enable Suspicion:**

```bash
python3 control.py switch gossip suspect
python3 control.py display_protocol
```

**Verify membership unchanged (still 8):**

```bash
python3 control.py list_mem
```

---

## Test 4 — Failures under Suspicion (Gossip + Suspicion)

### Kill two more **non-introducer** nodes (example: vm5 & vm9)

On **vm5**:

```bash
pkill -f "membership.py.*--port 9000"
```

On **vm9**:

```bash
pkill -f "membership.py.*--port 9000"
```

> You should see SUSPECT/DELETE immediately on peers’ stdout.

**After 10s, lists should show **6** members:**
On **vm1** and **vm6**:

```bash
python3 control.py list_mem
```

---

## Test 5 — Rejoin with a Different ID/Port

### Rejoin one of the failed nodes with a new port (example: vm7 → port **9001**)

On **vm7**:

```bash
cd /path/to/mp2
nohup python3 membership.py \
  --port 9001 \
  --introducer fa25-cs425-a801.cs.illinois.edu:9000 \
  --mode gossip --drop 0.0 --t_fail 2 --t_suspect 1.8 --t_cleanup 2 \
  > vm7_rejoin_p9001.log 2>&1 & echo $! > vm7_rejoin_p9001.pid
```

**After ~10s, membership should show **7**:**
On **vm2** and **vm6**:

```bash
python3 control.py list_mem
```

**Show vm7’s new id locally:**

```bash
python3 control.py list_self
python3 control.py list_mem
```

---

## Cleanup (optional, each VM)

```bash
pkill -f "membership.py.*--port 9"
```