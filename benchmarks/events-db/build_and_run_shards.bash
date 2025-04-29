#!/bin/bash

export CONTAINER_NAME="eventsql-benchmarks-events-postgres-0"
export EXPOSED_PORT=5432
bash build_and_run.bash

export CONTAINER_NAME="eventsql-benchmarks-events-postgres-1"
export EXPOSED_PORT=5433
bash build_and_run.bash

export CONTAINER_NAME="eventsql-benchmarks-events-postgres-2"
export EXPOSED_PORT=5434
bash build_and_run.bash
