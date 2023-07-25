#!/bin/bash

echo "Start the NiFi container"
docker run --name nifi -p 8080:8080 -p 8443:8443 --link mysql:mysql -d apache/nifi:1.12.0

# Function to check if the container is running
function is_container_running() {
  local status=$(docker container inspect -f '{{.State.Status}}' nifi 2>/dev/null)
  [[ "$status" == "running" ]]
}

echo "Waiting for the container to start running"
while ! is_container_running; do
  sleep 1
done

echo "Sleep 20 seconds to ensure the Nifi server is fully initialized"
sleep 20

echo "Container is running, execute commands inside the container"
docker exec nifi bash -c '
mkdir -p /opt/nifi/nifi-current/custom-jars
cd /opt/nifi/nifi-current/custom-jars

# Check if the JAR file already exists before downloading
if [ ! -f mysql-connector-java-5.1.17-bin.jar.zip ]; then
    wget http://java2s.com/Code/JarDownload/mysql/mysql-connector-java-5.1.17-bin.jar.zip
    unzip mysql-connector-java-5.1.17-bin.jar.zip
fi
exit
'


echo "Commands ran successfully..!!"

