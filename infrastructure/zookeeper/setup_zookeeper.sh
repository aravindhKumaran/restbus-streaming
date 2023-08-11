#!/bin/bash

# Start the Superset container in detached mode
echo "Starting Zookeeper container"

docker run -d --name zookeeper \
    -p 2181:2181 \
    -p 2888:2888 \
    -p 3888:3888 \
    debezium/zookeeper:1.6

echo "waiting until the container status is running"
function is_container_running() {
  local status=$(docker container inspect -f '{{.State.Status}}' zookeeper 2>/dev/null)
  [[ "$status" == "running" ]]
}

# Wait for the container to start running
while ! is_container_running; do
  sleep 1
done

echo "Zookeeper setup completed successfully!"

