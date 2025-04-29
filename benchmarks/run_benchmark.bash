#!/bin/bash
export EVENTS_TO_PUBLISH=${EVENTS_TO_PUBLISH:-100000}
export EVENTS_RATE=${EVENTS_RATE:-5000}

container_name="eventsql-benchmarks-runner"

docker stop $container_name
docker rm $container_name

docker run \
  -e EVENTS_TO_PUBLISH -e EVENTS_RATE \
  --network host --memory "2G" --cpus "2" \
  --name $container_name $container_name
