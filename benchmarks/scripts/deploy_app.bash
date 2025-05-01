#!/bin/bash
set -euo pipefail

echo "Deploying benchmarks app..."

export APP=benchmarks-app
export APP_DIR=app
export DEPLOY_HOST=$APP_HOST
bash deploy.bash
