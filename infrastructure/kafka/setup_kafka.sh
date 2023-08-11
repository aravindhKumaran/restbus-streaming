#!/bin/bash

echo "Starting Kafka container linked to ZooKeeper"

docker run -d --name kafka \
    -p 9092:9092 \
    --link zookeeper:zookeeper \
    debezium/kafka:1.6

echo "Waiting until the Kafka container status is running"
function is_kafka_running() {
  local status=$(docker container inspect -f '{{.State.Status}}' kafka 2>/dev/null)
  [[ "$status" == "running" ]]
}

# Wait for the Kafka container to start running
while ! is_kafka_running; do
  sleep 1
done

echo "Kafka setup completed successfully!"

# Execute commands inside the Kafka container
echo "Executing commands inside Kafka container"

docker exec -it kafka /bin/bash -c '
    cd bin
    echo "security.protocol=PLAINTEXT" > client.properties
    echo "Client properties file created successfully!"
'

echo "Commands executed inside Kafka container"

echo "Kafka setup completed successfully!"

