#!/bin/bash

container_name="eventsql-benchmarks-events-postgres"
volume_dir="${POSTGRESQL_VOLUME_DIR:-${HOME}/${container_name}_volume}"

docker build . -t $container_name

docker stop $container_name
docker rm $container_name

docker run -d -v "${volume_dir}:/var/lib/postgresql/data" -p "5543:5432" \
  -e "POSTGRES_USER=postgres" -e "POSTGRES_PASSWORD=postgres" \
  --memory "2G" --cpus "2" \
  --name $container_name $container_name
