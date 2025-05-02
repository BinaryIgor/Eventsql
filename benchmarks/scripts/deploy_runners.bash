#!/bin/bash
set -euo pipefail

echo "Deploying runner-0..."

export APP=bechmarks-runner
export APP_DIR=runner
export SKIP_RUNNING=true
export DEPLOY_HOST=$EVENTS_DB0_HOST
bash deploy.bash

echo
echo "runner-0 deployed, deploying runner-1..."

export DEPLOY_HOST=$EVENTS_DB1_HOST
bash deploy.bash

echo
echo "runner-1 deployed, deploying runner-2..."

export DEPLOY_HOST=$EVENTS_DB2_HOST
bash deploy.bash

echo
echo "All runners are deployed!"