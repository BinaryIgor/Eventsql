#!/bin/bash
set -euo pipefail

export RUNNER_INSTANCES=3
export RUNNER_HOST="$RUNNER0_HOST"
bash run_benchmark.bash

echo
export RUNNER_HOST="$RUNNER1_HOST"
bash run_benchmark.bash

echo
export RUNNER_HOST="$RUNNER2_HOST"
bash run_benchmark.bash
