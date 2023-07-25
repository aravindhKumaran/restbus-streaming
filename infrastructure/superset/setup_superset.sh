#!/bin/bash


# Start the Superset container in detached mode
echo "Startting superset container"
docker run -d -p 8088:8088 \
    -e "SUPERSET_SECRET_KEY=6FBD13AE6745FDBEB54BB53B56FC4" \
    --name superset apache/superset:2.1.0

echo "waiting until the container status is running"
function is_container_running() {
  local status=$(docker container inspect -f '{{.State.Status}}' mysql 2>/dev/null)
  [[ "$status" == "running" ]]
}

# Wait for the container to start running
while ! is_container_running; do
  sleep 1
done

echo "Sleep 10 seconds to ensure the MySQL server is fully initialized"
sleep 10

echo "Executing the superset user creation and updating database init commands"
# Execute commands inside the Superset container
docker exec -it superset superset fab create-admin \
    --username admin \
    --firstname Superset \
    --lastname Admin \
    --email admin@superset.com \
    --password admin

docker exec -it superset superset db upgrade

docker exec -it superset superset init

echo "Connect container started with name 'superset'."
echo "Superset setup completed successfully..!!"