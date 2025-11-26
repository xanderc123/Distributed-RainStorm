#!/bin/bash

python3 rainstorm_daemon.py \
  --mode leader \
  --logfile vm1.log

sleep 1 

echo "Raintorm daemon started successfully."
echo "PID saved to rainstorm_daemon_vm1.pid"
