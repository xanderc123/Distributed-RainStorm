# RainStorm Distributed System Testing Guide

## 🟢 Phase 1: Cleanup & Setup (Run on all VMs)

### 1. Clean Environment

```bash
# Kill all running RainStorm and Python processes to ensure a clean state
pkill -9 -f rainstorm; pkill -9 -f python3

# Remove old logs (Leader/Worker logs, Task logs) to clear history
rm -f leader.log worker.log task_*.log leader_console.log worker_console.log source_*.log
```

### 2. Start Membership Service

```bash
# Start Membership Protocol (Introducer/Member) in the background
nohup python3 membership.py --port 9000 --introducer fa25-cs425-9801.cs.illinois.edu:9000 --mode pingack --drop 0.0 --t_fail 2.5 --t_suspect 1.9 --t_cleanup 2 > membership_bg.log 2>&1 &

# (Optional) Verify membership list IDs
python control.py list_mem_ids

# (Optional) Kill membership if needed
pkill -f membership.py
```

### 3. Start RainStorm Cluster

```bash
# [On VM1] Start the Leader and worker process
nohup python3 rainstorm_daemon.py --mode leader --logfile leader.log > leader_console.log 2>&1 &
nohup python3 rainstorm_daemon.py --mode worker --logfile worker.log > worker_console.log 2>&1 &

# [On VM1, VM2, VM3, VM4,VM5,....VM10] Start the Worker process (VM1 runs both)
nohup python3 rainstorm_daemon.py --mode worker --logfile worker.log > worker_console.log 2>&1 &

# (Emergency) Kill Daemon only
pkill -f rainstorm_daemon.py
```

## 🧪 Phase 2: Test 0 - Basic Functionality (Identity)

**Goal**: Verify throughput and data integrity without data loss.

```bash
# Submit Job: 1 Stage, 3 Tasks/Stage, Identity Operator, Rate=100 tuples/sec
python3 rainstorm_client.py 1 3 identity "" dataset1.csv output_test0.txt true false 100

# 1. Check processing rate during execution (Should stay around 100)
tail -f leader.log

# 2. Verify Data Integrity after completion
# Count lines in original file
wc -l dataset1.csv

# Count lines in HyDFS output (Should be Source_Lines - 1 Header)
cat DFS/output_test0.txt/* | wc -l
```

## 🧪 Phase 3: Test 1 - Correctness (Filter & Count)

**Goal**: Verify that the logic for filtering and stateful aggregation is correct.

```bash
# Submit Job: 2 Stages, 3 Tasks/Stage, Filter "Sign" -> Aggregate Col 6, Rate=50
python3 rainstorm_client.py 2 3 filter "Sign" aggregate 6 dataset1.csv output_test1.txt true false 50

# 1. Verify RainStorm Result (Check the final accumulated counts at the end)
cat DFS/output_test1.txt/* | tail -n 20

# 2. Verify against Ground Truth (Run Python script to calculate locally)
python3 -c "import csv, sys; [print(next(csv.reader([line]))[6]) for line in sys.stdin if 'Sign' in line]" < dataset1.csv | sort | uniq -c

# Extract final counts for comparison
cat DFS/output_test1.txt/* | awk -F, '{print $1}' | sort | uniq -c > result_golden.log
```

## 🧪 Phase 4: Test 2 - Fault Tolerance (Kill Task)

**Goal**: Verify the system detects a failure and restarts the task automatically.

```bash
# 1. Submit Job: Same as Test 1, but output to a new file
python3 rainstorm_client.py 2 3 filter "Sign" aggregate 6 dataset1.csv output_test2.txt true false 50

# 2. List running tasks to find a victim (Look for Stage 1 Filter PID)
python3 rainstorm_client.py list_tasks

# 3. Kill the specific task (Replace <VM_HOSTNAME> and <PID> with actual values)
 python3 rainstorm_client.py kill_task fa25-cs425-980 PID


# 4. Monitor Leader Log for Recovery
# You should see "[Failure]..." followed by "[Recovery] Restarting Task..."
tail -f leader.log

cat DFS/output_test2.txt/* | awk -F, '{print $1}' | sort | uniq -c > result_golden.log
```

## 🧪 Phase 5: Test 3 - Autoscaling (App 2)

**Goal**: Verify the system scales up when load > HW (High Watermark).

```bash
# Submit Job: App 2 (Filter + Transform), Autoscale=True, Rate=1000, HW=5 (Low threshold to trigger scale up)
python3 rainstorm_client.py 2 1 filter "Sign" transform "cut1-3" dataset2.csv output_test3.txt false true 1000 2 5

# 1. Monitor Leader Log for "Scaling UP" events
tail -f leader.log

# 2. Check Worker Logs (Optional)
tail -f worker.log

# 3. Check HyDFS output (Verify content format is transformed/cut)
ls -lh DFS/
head -n 5 DFS/output_test3.txt/block-00001

# 4. Check Processes (Verify number of python processes increased)
ps -ef | grep rainstorm
```

## 📂 Phase 6: HyDFS Debugging (Optional Helper Commands)

If you need to debug the file system:

```bash
# Get a file from HyDFS to local
python3 hydfs_client.py --hosts hosts.txt get output_test.txt get_result.txt

# List files/blocks in HyDFS
python3 hydfs_client.py --hosts hosts.txt ls output_test.txt

# Put a local file to HyDFS
python3 hydfs_client.py --hosts hosts.txt put local_file.txt remote_file.txt

# Delete a file from HyDFS
python3 hydfs_client.py --hosts hosts.txt delete file.txt
```

## 📝 Additional Tips

### Monitoring Commands
```bash
# Check all running RainStorm processes
ps aux | grep rainstorm

# Check memory usage of Python processes
ps aux --sort=-%mem | grep python

# Monitor network connections
netstat -an | grep :9000

# Check system load
top -b -n 1 | head -20
```

### Log Analysis
```bash
# Search for specific patterns in logs
grep -i "error\|fail\|exception" leader.log
grep -i "complete\|success\|done" leader.log

# Count occurrences of events
grep -c "Task completed" worker.log
grep -c "Scaling" leader.log

# View recent log entries
tail -n 50 leader.log
tail -n 50 worker.log
```

### Performance Metrics
```bash
# Check file sizes
ls -lh DFS/
du -sh DFS/output_*.txt

# Count number of blocks
ls DFS/output_test3.txt/ | wc -l

# Check processing time
grep "Total processing time" leader.log
```

## 🔧 Troubleshooting

### Common Issues and Solutions

1. **Processes not starting**: Check if ports are already in use
   ```bash
   netstat -tulpn | grep :9000
   ```

2. **Connection errors**: Verify network connectivity between VMs
   ```bash
   ping fa25-cs425-9801.cs.illinois.edu
   ```

3. **Permission issues**: Ensure files have correct permissions
   ```bash
   chmod +x *.py
   chmod 644 *.csv *.log
   ```

4. **Disk space issues**: Check available storage
   ```bash
   df -h .
   ```

### Quick Restart Sequence
```bash
# Complete system restart
pkill -9 -f rainstorm; pkill -9 -f python3
sleep 2
rm -f *.log
nohup python3 membership.py --port 9000 --introducer fa25-cs425-9801.cs.illinois.edu:9000 --mode pingack --drop 0.0 --t_fail 2.5 --t_suspect 1.9 --t_cleanup 2 > membership_bg.log 2>&1 &
sleep 5
nohup python3 rainstorm_daemon.py --mode leader --logfile leader.log > leader_console.log 2>&1 &
nohup python3 rainstorm_daemon.py --mode worker --logfile worker.log > worker_console.log 2>&1 &
```


