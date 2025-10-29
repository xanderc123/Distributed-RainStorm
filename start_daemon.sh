#!/bin/bash

# --- 1. Start the Daemon ---
# nohup: Ensures the process continues to run even if the shell exits.
# > daemon_vm1.log 2>&1: Redirects standard output (1) and standard error (2) to the log file.
# &: Runs the entire command in the background.
# echo $! > daemon_vm1.pid: Gets the Process ID ($!) of the background job and saves it to a file.
nohup python3 membership.py \
  --port 9000 \
  --introducer fa25-cs425-9801.cs.illinois.edu:9000 \
  --mode pingack --drop 0.0 --t_fail 2 --t_suspect 1.8 --t_cleanup 2 \
  > daemon_vm1.log 2>&1 & echo $! > daemon_vm1.pid

# --- 2. Wait for Startup ---
# Gives the daemon a moment to initialize its threads (like the file server on 9002).
sleep 1 

echo "Daemon started successfully."
echo "PID saved to daemon_vm1.pid"
