#!/bin/bash

mvn clean install -P executable

container_name="eventsql-benchmarks-app"

docker build . -t $container_name

docker stop $container_name
docker rm $container_name

docker run -d --network host --memory "8G" --cpus "4" --name $container_name $container_name
