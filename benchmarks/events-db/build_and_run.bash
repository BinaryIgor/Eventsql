#!/bin/bash

container_name="${CONTAINER_NAME:-eventsql-benchmarks-events-postgres}"
volume_dir="${POSTGRESQL_VOLUME_DIR:-${HOME}/${container_name}_volume}"
exposed_port=${EXPOSED_PORT:-5432}

docker build . -t $container_name

docker stop $container_name
docker rm $container_name

docker run -d -v "${volume_dir}:/var/lib/postgresql/data" -p "${exposed_port}:5432" \
  -e "POSTGRES_USER=postgres" -e "POSTGRES_PASSWORD=postgres" \
  --memory "4G" --cpus "4" \
  --name $container_name $container_name
