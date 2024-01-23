#!/usr/bin/env bash

################################################################################
# This utility script can be used as a kubernetes liveness/readiness probe when
# using the python watch manager. A sample pod configuration looks like the
# following:
#
# spec:
#   ...
#   containers:
#     - name: operator
#       ...
#       env:
#         - name: WATCH_MANAGER
#           value: python
#         - name: PYTHON_WATCH_MANAGER_HEARTBEAT_FILE
#           value: /tmp/heartbeat.txt
#       livenessProbe:
#         exec:
#           command:
#             - check_heartbeat.sh
#             - /tmp/heartbeat/txt
#             - "120"
#       readinessProbe:
#         exec:
#           command:
#             - check_heartbeat.sh
#             - /tmp/heartbeat/txt
#             - "60"
################################################################################

if [ "$#" -lt "2" ]
then
    echo "Usage: $0 <heartbeat_file> <delta>"
    exit 1
fi

heartbeat_file=$1
delta=$2

stamp=$(date -d "$(cat $heartbeat_file)" +%s)
test $(expr "$stamp" + "$delta") -gt $(date +%s)
