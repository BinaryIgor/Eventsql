#!/bin/bash

mvn clean install

container_name="eventsql-benchmarks-runner"

docker build . -t $container_name
