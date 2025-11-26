#!/bin/bash

# --- 1. Start the Daemon ---
# nohup: Ensures the process continues to run even if the shell exits.
# > daemon_vm1.log 2>&1: Redirects standard output (1) and standard error (2) to the log file.
# &: Runs the entire command in the background.
# echo $! > daemon_vm1.pid: Gets the Process ID ($!) of the background job and saves it to a file.

ID=$1

# RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> … <opNstages_exe> <opNstages_args> <hydfs_src_directory> <hydfs_dest_filename> <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>
python3 rainstorm_client.py 1 3 filter ".*" "source_files/dataset1.csv" output1.log true false 100

# python3 rainstorm_client.py 1 3 filter ".*"
#   --introducer fa25-cs425-98${ID}.cs.illinois.edu:9000 \
#   --mode pingack --drop 0.0 --t_fail 2 --t_suspect 1.8 --t_cleanup 2 \
#   > daemon_vm${ID}.log 2>&1 & echo $! > daemon_vm${ID}.pid

# --- 2. Wait for Startup ---
# Gives the daemon a moment to initialize its threads (like the file server on 9002).
# sleep 1 

# echo "Daemon started successfully."
# echo "PID saved to daemon_vm${ID}.pid"
