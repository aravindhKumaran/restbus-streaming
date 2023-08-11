#!/bin/bash

# Function to check the status of the executed command
check_status() {
    if [ $? -ne 0 ]; then
        echo "Error occurred. Exiting..."
        exit 1
    fi
}

# Array of setup scripts
setup_scripts=(
    "mysql/setup_mysql.sh"
    "nifi/setup_nifi.sh"
    "zookeeper/setup_zookeeper.sh"
    "kafka/setup_kafka.sh"
    "msk-connect/setup_connect.sh"
    "spark/setup_spark.sh"  
)

# Base path where the setup scripts are located
base_path="/home/ubuntu/restbus-streaming/infrastructure/"

# Loop through the setup scripts and execute them
for script in "${setup_scripts[@]}"; do
    echo "Running $script..."
    sudo chmod +x "$base_path/$script"
    "$base_path/$script"
    check_status
done

echo "All scripts executed successfully!"