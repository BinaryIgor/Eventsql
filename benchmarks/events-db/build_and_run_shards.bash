#!/bin/bash

export CONTAINER_NAME="eventsql-benchmarks-events-postgres-0"
export EXPOSED_PORT=5555
bash build_and_run.bash

export CONTAINER_NAME="eventsql-benchmarks-events-postgres-1"
export EXPOSED_PORT=5556
bash build_and_run.bash

export CONTAINER_NAME="eventsql-benchmarks-events-postgres-2"
export EXPOSED_PORT=5557
bash build_and_run.bash
