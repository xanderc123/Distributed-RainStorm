#!/bin/bash

# Kill the HyDFS daemon process identified by the argument --port 9000.
# This stops all threads in that process, including the file server on 9002.
pkill -f "membership.py.*--port 9000" || true

# Wait one second for the operating system to clear the ports
sleep 1 

# Verify port closures (ss -l: listening, -n: numeric, -p: process, -t: TCP, -u: UDP)

# Check TCP Control Port (9900)
ss -lnpt | egrep ':9900' || echo 'no control 9900'

# Check UDP Membership Port (9000)
ss -lnup | egrep ':9000' || echo 'no udp 9000'

# Check TCP File Server Port (9002) - TCP used for client connection
ss -lnpt | egrep ':9002' || echo 'no file server 9002'
