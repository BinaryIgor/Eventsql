#!/bin/bash
set -euo pipefail

echo "Deploying events-db-0..."

export APP=events-db
export DEPLOY_HOST=$EVENTS_DB0_HOST
bash deploy.bash

echo
echo "events-db-0 deployed, deploying events-db-1..."

export DEPLOY_HOST=$EVENTS_DB1_HOST
bash deploy.bash

echo
echo "events-db-1 deployed, deploying events-db-2..."

export DEPLOY_HOST=$EVENTS_DB2_HOST
bash deploy.bash

echo
echo "All events dbs are deployed!"