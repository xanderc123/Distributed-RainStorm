#!/bin/bash

# --- 1. Start the Daemon ---
# nohup: Ensures the process continues to run even if the shell exits.
# > daemon_vm1.log 2>&1: Redirects standard output (1) and standard error (2) to the log file.
# &: Runs the entire command in the background.
# echo $! > daemon_vm1.pid: Gets the Process ID ($!) of the background job and saves it to a file.

PATTERN=$1
# "R[0-9]-[0-9]"
AGGREGATE_COLUMN=$2
# 10 

# RainStorm <Nstages> <Ntasks_per_stage> <op1_exe> <op1_args> … <opNstages_exe> <opNstages_args> <hydfs_src_directory> <hydfs_dest_filename> <exactly_once> <autoscale_enabled> <INPUT_RATE> <LW> <HW>
python3 rainstorm_client.py 2 3 filter ${PATTERN} aggregate ${AGGREGATE_COLUMN} "source_files/dataset0.csv" output1.log true false 100
