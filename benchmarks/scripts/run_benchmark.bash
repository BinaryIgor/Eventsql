#!/bin/bash
set -euo pipefail

remote_host="eventsql@$RUNNER_HOST"
ssh -oStrictHostKeyChecking=accept-new "${remote_host}" "
  cd /home/eventsql/deploy/runner/latest
  export EVENTS_RATE=${EVENTS_RATE}
  export EVENTS_TO_PUBLISH=${EVENTS_TO_PUBLISH}
  export RUNNER_INSTANCES=${RUNNER_INSTANCES}
  bash load_and_run_app.bash"

echo
echo "Benchmark is running. To check results, run:"
echo "ssh $remote_host 'docker logs benchmarks-runner'"
