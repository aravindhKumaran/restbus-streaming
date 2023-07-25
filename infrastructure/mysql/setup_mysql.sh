#!/bin/bash

#environment variables
MYSQL_ROOT_PASSWORD=debezium
MYSQL_USER=mysqluser
MYSQL_PASSWORD=mysqlpw

# Start the MySQL container in detached mode
echo "Startting MySql container"
docker run -dit --name mysql -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD} \
  -e MYSQL_USER=${MYSQL_USER}  \
  -e MYSQL_PASSWORD=${MYSQL_PASSWORD}  \
  debezium/example-mysql:1.6

echo "waiting until the container status is running"
function is_container_running() {
  local status=$(docker container inspect -f '{{.State.Status}}' mysql 2>/dev/null)
  [[ "$status" == "running" ]]
}

# Wait for the container to start running
while ! is_container_running; do
  sleep 1
done

echo "Sleep 30 seconds to ensure the MySQL server is fully initialized"
sleep 30

echo "Executing the sql commands"
# Execute commands inside the MySQL container
docker exec -i mysql mysql -u root -p"$MYSQL_ROOT_PASSWORD" << SQL_EOF
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

CREATE TABLE IF NOT EXISTS bus_status (
    record_id INT NOT NULL AUTO_INCREMENT,
    id INT NOT NULL,
    routeId INT NOT NULL,
    directionId VARCHAR(40),
    predictable BOOLEAN,
    secsSinceReport INT NOT NULL,
    kph INT NOT NULL,
    heading INT,
    lat REAL NOT NULL, 
    lon REAL NOT NULL,
    leadingVehicleId INT,
    event_time DATETIME DEFAULT NOW(),
    PRIMARY KEY (record_id)
);
SQL_EOF


echo "script ran successfully, and db is created"