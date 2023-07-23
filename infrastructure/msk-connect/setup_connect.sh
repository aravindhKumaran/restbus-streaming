#!/bin/bash

# environment variables
# export BOOTSTRAP_SERVERS="b-3.streamingmsk.64mbdl.c2.kafka.ca-central-1.amazonaws.com:9092,b-1.streamingmsk.64mbdl.c2.kafka.ca-central-1.amazonaws.com:9092,b-2.streamingmsk.64mbdl.c2.kafka.ca-central-1.amazonaws.com:9092"
# export ZOOKEEPER_CONNECTION_STRING="z-3.streamingmsk.64mbdl.c2.kafka.ca-central-1.amazonaws.com:2181,z-2.streamingmsk.64mbdl.c2.kafka.ca-central-1.amazonaws.com:2181,z-1.streamingmsk.64mbdl.c2.kafka.ca-central-1.amazonaws.com:2181"

# Run the Connect container
echo "Creating connect-msk container"
docker run -dit --name connect-msk -p 8083:8083 \
  -e GROUP_ID=1 \
  -e CONFIG_STORAGE_TOPIC=my-connect-configs \
  -e OFFSET_STORAGE_TOPIC=my-connect-offsets \
  -e STATUS_STORAGE_TOPIC=my-connect-statuses \
  -e BOOTSTRAP_SERVERS=$BOOTSTRAP_SERVERS \
  -e KAFKA_VERSION=2.8.1 \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=2 \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=2 \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=2 \
  --link mysql:mysql debezium/connect:1.8.0.Final

echo "Connect container started with name 'connect-msk'."
echo "msk-connect setup completed successfully..!!"